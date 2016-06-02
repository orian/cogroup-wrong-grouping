package eu.pawelsz.apache.beam;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.runners.flink.FlinkPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class CreateData {

  @DefaultCoder(AvroCoder.class)
  public static class DumbData {
    @Nullable
    String key1;
    @Nullable
    Long key2;
    @Nullable
    Long value1;

    public DumbData() {
    }
  }

  @DefaultCoder(AvroCoder.class)
  static class Config {
    int key1;
    int key2;
    int perKey;
    long val;

    public Config() {
    }

    public Config(int k1, int k2, int pk, long v) {
      key1 = k1;
      key2 = k2;
      perKey = pk;
      val = v;
    }

    public static List<Config> of(int k1, int k2, int pk, long v, int shards) {
      pk = pk/shards;
      List<Config> c = new LinkedList<>();
      for (int i=0;i<shards;i++) {
        c.add(new Config(k1,k2,pk,v));
      }
      return c;
    }
  }

  static class Generator extends DoFn<Config, DumbData> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      Config cfg = c.element();

      ArrayList<String> bs = new ArrayList<>(cfg.key1);
      for (int i = 0; i < cfg.key1; i++) {
        bs.add("hereGoesLongStringID" + i);
      }

      for (int k = 0; k<cfg.perKey; k++) {
       for (long j = 0; j < cfg.key2; j++) {
          for (int i = 0; i < cfg.key1; i++) {
            DumbData dd = new DumbData();
            dd.key1 = bs.get(i);
            dd.key2 = j;
            dd.value1 = cfg.val;
            c.output(dd);
          }
        }
      }
    }
  }

  private static final TupleTag<Long> tag1 = new TupleTag<>();
  private static final TupleTag<Long> tag2 = new TupleTag<>();

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    options.setRunner(FlinkPipelineRunner.class);
    Pipeline p = Pipeline.create(options);

    p.apply(Create.of(Config.of(4, 100, 1, 1, 6)))
        .apply(ParDo.of(new Generator())).apply(
            AvroIO.Write.to("/tmp/dataset1").withSchema(DumbData.class).withNumShards(6));

    p.apply(Create.of(Config.of(4, 100, 100000, 2, 6))).
        apply(ParDo.of(new Generator())).apply(
            AvroIO.Write.to("/tmp/dataset2").withSchema(DumbData.class).withNumShards(6));

    p.run();
  }
}
