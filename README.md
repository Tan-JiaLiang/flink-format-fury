# Flink Fury Format
The Apache Fury format allows to read and write Fury data based on an Fury schema. Currently, the Fury schema is derived from table schema.

# Building from Source
```
git clone https://github.com/Tan-JiaLiang/flink-format-fury.git
cd flink-format-fury
mvn clean package -DskipTests
```

# Example
```sql
CREATE TABLE source_table (
  id INT,
  name STRING,
  age BIGINT
) WITH (
  'connector' = 'datagen',
  'fields.id.min' = '0',
  'fields.id.max' = '10',
  'fields.name.length' = '8',
  'fields.age.min' = '18',
  'fields.age.max' = '85',
  'rows-per-second' = '10'
);

CREATE TABLE sink_table (
  id INT,
  name STRING,
  age BIGINT
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = 'localhost:9092',
  'topic' = 'fury-test',
  'format' = 'fury',
  'fury.ignore-parse-errors' = 'false'
);

INSERT INTO sink_table
SELECT * FROM source_table;
```

# Format Options
<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>ignore-parse-errors</h5></td>
      <td>optional</td>
      <td>false</td>
      <td>Boolean</td>
      <td>Optional flag to skip fields and rows with parse errors instead of failing.</td>
    </tr>
    </tbody>
</table>

# Data Type Mapping
<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">Arrow type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>STRING</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>INT32</code></td>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>INT64</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>BOOLEAN</code></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>BINARY</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>DECIMAL</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>INT8</code></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>INT16</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>FLOAT32</code></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>FLOAT64</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>INT32</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>INT32</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>INT64</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>ARRAY</code></td>
    </tr>
    <tr>
      <td><code>MAP</code></td>
      <td><code>MAP</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>STRUCT</code></td>
    </tr>
    </tbody>
</table>
