package org.example;

import com.google.auto.value.AutoValue;
import com.google.auto.service.AutoService;

import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;

import org.apache.beam.sdk.managed.Managed;
import static org.apache.beam.sdk.managed.Managed.ICEBERG_CDC;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.io.Serializable;


@AutoService(SchemaTransformProvider.class)
public class ReadIcebergCDCProvider
    extends TypedSchemaTransformProvider<ReadIcebergCDCProvider.Configuration> {

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  public String identifier() {
    return "beam:transform:iceberg:cdc_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    // No input PCollections for a source read
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @Override
  protected SchemaTransform from(Configuration cfg) {
    return new SchemaTransform() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        // Build config map for Iceberg CDC
        Map<String, Object> cfgMap = new HashMap<>();
        cfgMap.put("table", cfg.getTable());
        cfgMap.put("catalog_name", cfg.getCatalogName());
        cfgMap.put("poll_interval_seconds", cfg.getPollIntervalSeconds());

        Map<String, String> props = new HashMap<>();
//        props.put("type", "rest");
//        props.put("uri", cfg.getUri());
        props.put("warehouse", cfg.getWarehouse());
        props.put("catalog-impl",
            "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog");
        props.put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO");
        props.put("gcp_project", "apache-beam-testing");
        props.put("gcp_location", "us-central1");

        cfgMap.put("catalog_properties", props);
        cfgMap.put("config_properties", Map.of());
        // CDC requires streaming read
        cfgMap.put("streaming", true);

        // Apply the Iceberg CDC read
        Pipeline p = input.getPipeline();
        PCollection<Row> rows = p.apply(Managed.read(ICEBERG_CDC).withConfig(cfgMap))
            .getSinglePCollection();
        // Tag output
        return PCollectionRowTuple.of("output", rows);
      }
    };
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration implements Serializable {
    public abstract String getTable();
    public abstract String getCatalogName();
    public abstract String getUri();
    public abstract String getWarehouse();
    public abstract int getPollIntervalSeconds();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Configuration build();
      public abstract Builder setTable(String t);
      public abstract Builder setCatalogName(String c);
      public abstract Builder setUri(String u);
      public abstract Builder setWarehouse(String w);
      public abstract Builder setPollIntervalSeconds(int p);
    }
  }
}
