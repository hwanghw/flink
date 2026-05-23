# Flink Serialization & Deserialization Deep Dive

## Table of Contents
1. [When Does Serialization/Deserialization Happen?](#1-when-does-serializationdeserialization-happen)
2. [Flink's Type System & Serializer Hierarchy](#2-flinks-type-system--serializer-hierarchy)
3. [Field Types Natively Serializable by Flink](#3-field-types-natively-serializable-by-flink)
4. [Performance Comparison](#4-performance-comparison)
5. [POJO Serialization](#5-pojo-serialization)
6. [Kryo Serialization](#6-kryo-serialization)
7. [How to Detect Kryo Usage](#7-how-to-detect-kryo-usage-metrics-logging-runtime)
8. [Protobuf Serialization](#8-protobuf-serialization)
9. [Best Practice Recommendations](#9-best-practice-recommendations)

---

## 1. When Does Serialization/Deserialization Happen?

Serialization is pervasive in Flink. It occurs at every boundary where data must be transferred, persisted, or recovered.

```
                       Serialization Points in a Flink Job
                       ===================================

  Job Submission                     Runtime Execution
  +--------------+      +----------------------------------------------------+
  | UDF classes   |      |                                                    |
  | serialized &  |      |  Operator A --(ser)--> Network --(deser)--> Operator B
  | sent to TMs   |      |      |                 Buffer                  |   |
  +--------------+      |      |                                          |   |
                         |      v                                          v   |
                         |  State Read/Write                        State Read/Write
                         |  (ser/deser on                           (ser/deser on
                         |   every access                            every access
                         |   for RocksDB)                            for RocksDB)
                         |      |                                          |   |
                         |      v                                          v   |
                         |  Checkpoint/Savepoint                   Checkpoint/Savepoint
                         |  (ser to durable                        (ser to durable
                         |   storage)                               storage)
                         +----------------------------------------------------+
```

### Detailed Breakdown

| Serialization Point | When It Happens | Impact |
|---|---|---|
| **Network shuffle** | Data flows between operators on different task slots | Every record serialized into fixed-size buffers, deserialized on receive. Pipelined for streaming, blocking/hybrid for batch. |
| **State backend read/write** | Every `valueState.value()` / `.update()` call | **RocksDB**: ser/deser on EVERY access + disk I/O (order of magnitude slower). **HashMapStateBackend**: ser/deser required. **Heap (in-memory)**: NO ser/de overhead. |
| **Checkpointing** | Periodic snapshots to durable storage | Full state serialized; incremental checkpoints reduce volume but still require ser/de. |
| **Savepointing** | Manual snapshots (canonical or native format) | Full state serialized in portable format. Flink 1.15+ supports faster native format. |
| **UDF distribution** | Job submission, before execution starts | User functions serialized and sent to TaskManagers for execution. |
| **Broadcast state** | Broadcast stream processing | Serialized and distributed to all parallel instances. |

**Key insight**: For stateful jobs using RocksDB, serialization cost dominates — every single state access involves ser/deser plus potential disk I/O. Optimizing the serializer here gives the biggest payoff.

---

## 2. Flink's Type System & Serializer Hierarchy

Flink uses a two-step process: `TypeInformation<T>` (determined at pre-flight via reflection) produces a `TypeSerializer<T>` (used at runtime for actual ser/de).

```
  Flink Type Detection (in order of precedence)
  ================================================

  Your Class
      |
      v
  +------------------------+
  | Is it a basic type?     |--Yes--> BasicTypeSerializer (int, String, etc.)
  | (primitives, String)    |         [FASTEST - no reflection]
  +----------+-------------+
             | No
             v
  +------------------------+
  | Is it a Flink Tuple?    |--Yes--> TupleSerializer
  | (Tuple0..Tuple25)       |         [FASTEST - direct field access]
  +----------+-------------+
             | No
             v
  +------------------------+
  | Is it a Flink Row?      |--Yes--> RowSerializer
  |                         |         [FAST - specialized code]
  +----------+-------------+
             | No
             v
  +------------------------+
  | Does it meet POJO       |--Yes--> PojoSerializer
  | requirements?           |         [FAST - field-by-field, no reflection at runtime]
  +----------+-------------+
             | No
             v
  +------------------------+
  | Registered custom       |--Yes--> Custom TypeSerializer
  | serializer?             |         [Performance depends on implementation]
  +----------+-------------+
             | No
             v
  +------------------------+
  | KRYO FALLBACK           |         GenericTypeSerializer (wraps Kryo)
  | (generic type)          |         [SLOW - 50-75% throughput loss]
  +------------------------+
```

### How TypeInformation is determined:
1. **Pre-flight phase**: Flink analyzes function signatures, generic parameters, and subclass info via reflection
2. `TypeInformation.createSerializer(executionConfig)` produces the runtime `TypeSerializer`
3. If type extraction fails or the type is unrecognized → Kryo fallback
4. You can override with `ResultTypeQueryable` interface or `.returns()` hint

---

## 3. Field Types Natively Serializable by Flink

These types have dedicated fast serializers and do NOT fall back to Kryo:

### Basic / Primitive Types
| Type | Serializer |
|---|---|
| `byte`, `short`, `int`, `long`, `float`, `double`, `boolean`, `char` | Primitive type serializers |
| `Byte`, `Short`, `Integer`, `Long`, `Float`, `Double`, `Boolean`, `Character` | Boxed type serializers |
| `String` | StringSerializer |
| `void` | VoidSerializer |

### Numeric Types
| Type | Serializer |
|---|---|
| `java.math.BigDecimal` | BigDecSerializer |
| `java.math.BigInteger` | BigIntSerializer |

> **Warning**: Scala's `BigDecimal` falls back to Kryo. Use `java.math.BigDecimal` instead.

### Date / Time Types
| Type | Serializer |
|---|---|
| `java.sql.Date` | SqlDateSerializer |
| `java.sql.Time` | SqlTimeSerializer |
| `java.sql.Timestamp` | SqlTimestampSerializer |
| `java.time.LocalDate` | LocalDateSerializer |
| `java.time.LocalTime` | LocalTimeSerializer |
| `java.time.LocalDateTime` | LocalDateTimeSerializer |
| `java.time.Instant` | InstantSerializer |

### Array Types
| Type | Serializer |
|---|---|
| Primitive arrays (`byte[]`, `int[]`, `long[]`, etc.) | PrimitiveArraySerializer |
| `String[]` | StringArraySerializer |
| Object arrays of supported types | GenericArraySerializer |

### Collection Types (with concrete type arguments)
| Type | Requirement | Serializer |
|---|---|---|
| `List<T>` | T must be Flink-serializable, use interface type | ListSerializer |
| `Map<K, V>` | K, V must be Flink-serializable | MapSerializer |
| `Set<T>` | T must be Flink-serializable | SetSerializer |

> **Important**: Must use concrete type arguments (`List<String>`, not raw `List` or `List<?>`). Flink does NOT preserve the underlying collection implementation class.

### Composite Types
| Type | Serializer | Notes |
|---|---|---|
| `Tuple0` through `Tuple25` | TupleSerializer | Fastest. No null fields. Max 25 fields. |
| `Row` | RowSerializer | Supports nulls. Arbitrary field count. |
| POJO classes | PojoSerializer | Must meet all POJO requirements (see §5) |
| Scala case classes | CaseClassSerializer | If they meet POJO-like requirements |

### Special Flink Types
| Type | Notes |
|---|---|
| `Either<L, R>` | Native support for left/right union type |
| `Enum` types | Supported natively |
| Hadoop `Writable` | Uses `write()` / `readFields()` methods |
| Flink `Value` types (`IntValue`, `StringValue`, `LongValue`, etc.) | Mutable variants of basic types, useful for reuse/performance |
| `CopyableValue` | Manual internal cloning support |

### Types That Fall Back to Kryo
| Type | Why |
|---|---|
| `java.util.UUID` | No native serializer |
| `java.util.Optional` | No native serializer |
| Scala `BigDecimal` | Use `java.math.BigDecimal` instead |
| Scala sealed traits / ADTs | Not recognized as POJOs |
| Protobuf generated classes | Private internal fields (`fieldName_`) |
| Guava collections | Third-party types |
| Any class not meeting POJO requirements | See §5 for requirements |

---

## 4. Performance Comparison

| Rank | Serializer | Relative Throughput (vs Tuple) | Notes |
|------|-----------|------|-------|
| 1 | **Tuple / Row** | 100% (baseline) | Direct field access, no reflection |
| 2 | **POJO** | ~70% | Field-by-field serialization, schema evolution support |
| 3 | **Protobuf** (native) | ~70% | Code-generated ser/de |
| 4 | **Avro** | ~70-80% | Similar to Protobuf |
| 5 | **Kryo (registered)** | ~25% | Integer tag instead of full class name |
| 6 | **Kryo (unregistered)** | ~17% | Writes full class name per record — massive overhead |

**Real-world impact**: Shopify reported a **20% throughput increase** after eliminating Kryo fallbacks in their Flink jobs.

### Serialized size comparison
- Binary formats (Avro, Protobuf) produce payloads **40-60% smaller** than JSON
- Unregistered Kryo adds ~50+ bytes per record just for the fully qualified class name

---

## 5. POJO Serialization

### Requirements (ALL must be met)

1. **Public class** — not a non-static inner class
2. **Public no-arg constructor** — `public MyClass() {}`
3. **All non-static, non-transient fields** must be either:
   - `public` (and non-final), OR
   - Have `public` getter AND setter following JavaBean naming (`getFoo()` / `setFoo()`)
4. **All field types** must themselves be serializable by Flink (see §3 above)

### Nested POJOs
Supported — if the nested class ALSO meets all POJO requirements. If a nested field doesn't qualify as a POJO, **only that field** falls back to Kryo (the parent can still be a POJO for its other fields).

### When POJO is the right choice
- Custom domain objects in DataStream API
- You control the class definition
- You need **state schema evolution** (POJO supports field addition/removal/reordering)
- Best performance/flexibility ratio without external frameworks

### Verification in unit tests
```java
import org.apache.flink.types.PojoTestUtils;

// Verify recognized as POJO
PojoTestUtils.assertSerializedAsPojo(MyClass.class);

// Stricter: ensure NO fields use Kryo
PojoTestUtils.assertSerializedAsPojoWithoutKryo(MyClass.class);
```

### Common POJO mistakes
```java
// BAD: non-static inner class
public class Outer {
    public class Inner { ... }  // Falls back to Kryo
}

// BAD: missing no-arg constructor
public class MyEvent {
    public MyEvent(String id) { ... }  // No default constructor → Kryo
}

// BAD: private fields without getters/setters
public class MyEvent {
    private String id;  // No getId()/setId() → Kryo
}

// GOOD: proper POJO
public class MyEvent {
    public String id;
    public long timestamp;
    public MyEvent() {}  // public no-arg constructor
}

// ALSO GOOD: JavaBean style
public class MyEvent {
    private String id;
    private long timestamp;
    public MyEvent() {}
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
```

---

## 6. Kryo Serialization

### When Flink falls back to Kryo
- Class doesn't meet any POJO requirement
- No registered custom serializer for the type
- Third-party library types (Scala BigDecimal, Guava collections, etc.)
- Protobuf generated classes (private internal fields like `impressionId_`)
- Generic types Flink cannot analyze via reflection

### Why Kryo is slow

```
  Unregistered Kryo: writes full class name with EVERY record
  ============================================================

  Record 1: [com.example.package.MyEvent][field1][field2][field3]
  Record 2: [com.example.package.MyEvent][field1][field2][field3]
  Record 3: [com.example.package.MyEvent][field1][field2][field3]
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             ~50+ bytes overhead PER RECORD

  Registered Kryo: writes integer tag instead
  ============================================

  Record 1: [42][field1][field2][field3]
  Record 2: [42][field1][field2][field3]
  Record 3: [42][field1][field2][field3]
             ^^
             ~4 bytes (still slower than POJO due to Kryo runtime overhead)
```

### How to register types with Kryo
```java
// Register type (uses integer tag instead of full class name)
env.getConfig().registerKryoType(MyClass.class);

// Register with custom Kryo serializer
env.getConfig().registerTypeWithKryoSerializer(
    MyClass.class, MyKryoSerializer.class);
```

### Kryo limitations
- **No state schema evolution** — cannot change class structure between savepoint restore
- Even registered Kryo is **64% slower** than POJO
- Checkpoint/savepoint stores Kryo registration mappings for consistency

---

## 7. How to Detect Kryo Usage (Metrics, Logging, Runtime)

### Method 1: Disable Kryo Fallback (Best for dev/test)

```java
// In your job code
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().disableGenericTypes();
```

Or via `flink-conf.yaml`:
```yaml
pipeline.generic-types: false
```

**What happens**: Flink throws `UnsupportedOperationException` with the exact class name:
```
java.lang.UnsupportedOperationException:
  Generic types have been disabled in the ExecutionConfig and type
  com.example.MyEvent is treated as a generic type.
```

The stack trace pinpoints: the class name, the operator where it's used, and the code path.

### Method 2: TypeExtractor Logging (DEBUG level)

Add to your Log4j properties:
```properties
logger.typeextractor.name = org.apache.flink.api.java.typeutils.TypeExtractor
logger.typeextractor.level = DEBUG
```

**Warning message to watch for**:
```
Class com.example.MyEvent cannot be used as a POJO type because not all
fields are valid POJO fields, and must be processed as GenericType.
```

> Only enable DEBUG temporarily — it generates heavy log traffic.

### Method 3: Programmatic TypeInformation Check

```java
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(MyClass.class);

if (typeInfo instanceof GenericTypeInfo) {
    System.out.println("WARNING: " + typeInfo + " uses Kryo serialization!");
} else {
    System.out.println("OK: " + typeInfo + " uses native serialization.");
}
```

You can also inspect the serializer directly:
```java
TypeSerializer<?> serializer = typeInfo.createSerializer(env.getConfig());
System.out.println("Serializer class: " + serializer.getClass().getName());
// If it's KryoSerializer → Kryo is being used
```

### Method 4: PojoTestUtils in Unit Tests

```java
import org.apache.flink.types.PojoTestUtils;

@Test
public void ensureNoKryo() {
    // Fails if class is not recognized as POJO
    PojoTestUtils.assertSerializedAsPojo(MyEvent.class);

    // Fails if ANY field within the POJO uses Kryo
    PojoTestUtils.assertSerializedAsPojoWithoutKryo(MyEvent.class);
}
```

### Method 5: Async-Profiler / Flamegraph (Production)

Use async-profiler to generate flamegraphs of a running Flink job. Kryo classes appear distinctly in the flamegraph with high CPU usage in:
- `com.esotericsoftware.kryo.*`
- `org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer.*`

This is the most effective way to detect **unexpected** Kryo usage in production without modifying the job.

### Method 6: Flink Web UI

The Flink Web UI (`:8081`) does **NOT** directly show serializer information. However:
- Task exceptions tab shows serialization errors
- Backpressure indicators can hint at serialization bottlenecks
- FLIP-398 (Flink 2.0+) proposes improved serialization visibility

### Summary of Detection Methods

| Method | Environment | Invasiveness | What It Detects |
|---|---|---|---|
| `disableGenericTypes()` | Dev / Test | Fails the job | All Kryo usage with exact class names |
| TypeExtractor DEBUG log | Dev / Test | Log noise | POJO validation failures |
| Programmatic TypeInfo check | Dev / Test | None | Per-class serializer type |
| `PojoTestUtils` | Unit tests | None | POJO compliance per class |
| Async-profiler flamegraph | Production | Low | Kryo CPU hotspots at runtime |
| Custom metrics | Production | Code change | Serialization latency per operator |

### Recommended Workflow

```
  Development         Testing              Production
  ===========         =======              ==========

  disableGenericTypes()                     async-profiler
       |              PojoTestUtils              |
       v              in unit tests              v
  Fix all Kryo             |              flamegraph analysis
  fallbacks                v                     |
       |              TypeExtractor              v
       v              DEBUG logging        investigate hotspots
  Register types           |                     |
  or fix POJOs             v                     v
       |              CI gate: fail         custom metrics
       v              if Kryo detected      for ongoing
  Verified clean                            monitoring
```

---

## 8. Protobuf Serialization

### Why Protobuf classes can never be POJOs
Protobuf generated classes have private internal fields with underscore naming (e.g., `impressionId_`), no standard getters/setters, and builder-based construction — none of which meets Flink POJO requirements.

### Approach A: Flink Native Protobuf Format (Table API / SQL)

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-protobuf</artifactId>
  <version>${flink.version}</version>
</dependency>
```

```sql
CREATE TABLE my_table (
    id BIGINT,
    name STRING,
    event_time TIMESTAMP(3)
) WITH (
    'format' = 'protobuf',
    'protobuf.message-class-name' = 'com.example.MyMessage'
);
```

**Configuration options:**

| Option | Default | Description |
|---|---|---|
| `protobuf.message-class-name` | (required) | Fully qualified proto class name |
| `protobuf.ignore-parse-errors` | false | Skip unparseable rows |
| `protobuf.read-default-values` | false | Read empty as proto defaults (impacts perf) |
| `protobuf.write-null-string-literal` | `""` | Placeholder for null strings in maps/arrays |

**Type mappings:**

| Flink SQL Type | Protobuf Type |
|---|---|
| STRING | string |
| BOOLEAN | bool |
| BYTES | bytes |
| INT | int32 |
| BIGINT | int64 |
| FLOAT | float |
| DOUBLE | double |
| ARRAY | repeated |
| MAP | map |
| ROW | message |

### Approach B: Kryo + chill-protobuf (DataStream API)

```xml
<dependency>
  <groupId>com.twitter</groupId>
  <artifactId>chill-protobuf</artifactId>
  <version>0.7.6</version>
</dependency>
<dependency>
  <groupId>com.google.protobuf</groupId>
  <artifactId>protobuf-java</artifactId>
  <version>3.7.0</version>
</dependency>
```

```java
env.getConfig().registerTypeWithKryoSerializer(
    MyProtoMessage.class,
    com.twitter.chill.protobuf.ProtobufSerializer.class
);
```

### Approach C: Custom DeserializationSchema (DataStream + Kafka)

```java
public class ProtoDeserializer<T extends Message>
        implements DeserializationSchema<T> {

    private final Class<T> protoClass;
    private transient Method parseFrom;

    public ProtoDeserializer(Class<T> protoClass) {
        this.protoClass = protoClass;
    }

    @Override
    public void open(InitializationContext ctx) throws Exception {
        parseFrom = protoClass.getMethod("parseFrom", byte[].class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] bytes) throws IOException {
        try {
            return (T) parseFrom.invoke(null, bytes);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize protobuf", e);
        }
    }

    @Override
    public boolean isEndOfStream(T t) { return false; }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(protoClass);
    }
}
```

### Protobuf Pitfalls

| Pitfall | Problem | Solution |
|---|---|---|
| POJO recognition | Proto classes can never be POJOs | Accept; use chill-protobuf or custom serializer |
| Null values | Protobuf doesn't support nulls in maps/arrays | Configure `write-null-string-literal`; validate pre-serialization |
| State schema evolution | Not supported with Kryo-wrapped Protobuf | Use Avro or POJO for stateful apps needing schema changes |
| ClassNotFoundException | Kryo's default JavaSerializer uses wrong classloader | Use `org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer` |
| OneOf handling | Serialization order matters in Table API | Higher-positioned fields override lower ones in same group |
| Missing registration | Forgetting to register → unregistered Kryo fallback | Always register explicitly |

### Schema Evolution with Protobuf
- Protobuf itself supports forward/backward compatible changes (add optional fields, don't change field numbers)
- **But**: Flink state does NOT support schema evolution for Kryo-serialized types
- For stateful apps needing schema evolution: use **POJO or Avro** instead
- For Kafka source/sink format evolution: use schema registry (Confluent etc.) with BACKWARD_TRANSITIVE compatibility

---

## 9. Best Practice Recommendations

### Decision Tree

```
  Choosing Your Serializer
  =========================

  Q: Are you using Table API / SQL?
      |
      +-- Yes --> Use native format (protobuf, avro, json, etc.)
      |           Flink handles ser/de automatically
      |
      +-- No (DataStream API)
           |
           Q: Do you control the data class?
               |
               +-- Yes --> Make it a POJO
               |           + Schema evolution support
               |           + ~70% of Tuple throughput
               |           + No external dependencies
               |
               +-- No (Protobuf, 3rd party, etc.)
                    |
                    Q: Is it Protobuf?
                        |
                        +-- Yes --> Register with chill ProtobufSerializer
                        |           OR write custom TypeSerializer
                        |
                        +-- No --> Register with Kryo (registerKryoType)
                                   Last resort, still 64% slower than POJO
```

### Top Rules

1. **Always run `disableGenericTypes()` in dev/test** — catch Kryo fallbacks before they reach production
2. **Add `PojoTestUtils` assertions in unit tests** — CI gate against Kryo regressions
3. **Prefer POJO** for custom types in DataStream API — best performance/flexibility ratio
4. **If using Protobuf in DataStream**: register with `chill-protobuf`; never leave unregistered
5. **Never leave types unregistered with Kryo** in production — 75% throughput loss
6. **For stateful apps needing schema evolution**: use POJO or Avro (NOT Kryo, NOT Protobuf)
7. **RocksDB state backend** amplifies serialization cost — optimize serializer choice here first
8. **Use Flink Tuples or Rows** for internal intermediate data where you don't need named fields
9. **Avoid Scala-specific types** (BigDecimal, sealed traits) when possible — they fall back to Kryo
10. **Monitor in production** with async-profiler flamegraphs to catch unexpected Kryo hotspots
