package eu.pawelsz.apache.beam;

import com.google.common.collect.Lists;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CoGroupPipeline {

  @DefaultCoder(AvroCoder.class)
  static class Key {
    @Nullable
    String key1;
    @Nullable
    Long key2;

    public Key() {
    }

    public Key(String s, Long k) {
      key1 = s;
      key2 = k;
    }
  }

  @DefaultCoder(AvroCoder.class)
  static class Container {
    int tag;

    @Nullable
    CreateData.DumbData val;

    public Container() {
    }

    public Container(int tag, CreateData.DumbData val) {
      this.tag = tag;
      this.val = val;
    }
  }

  static class MakeKey extends SimpleFunction<CreateData.DumbData, Key> {

    @Override
    public Key apply(CreateData.DumbData dd) {
      return new Key(dd.key1, dd.key2);
    }
  }

  public static class Merge extends DoFn<KV<Key, CoGbkResult>, String> {

    private static final Logger LOG = LoggerFactory.getLogger(Merge.class);

    private final Aggregator<Long, Long> missD1Cnt =
        createAggregator("missing data1", new Sum.SumLongFn());

    private final Aggregator<Long, Long> missD2Cnt =
        createAggregator("missing data2", new Sum.SumLongFn());

    private final Aggregator<Long, Long> haveBoth =
        createAggregator("have data from both sources", new Sum.SumLongFn());


    private final Aggregator<Long, Long> itemCnt =
        createAggregator("item count", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) throws Exception {
      KV kv = c.element();
      Key key = c.element().getKey();
      CoGbkResult res = c.element().getValue();
      Iterable<CreateData.DumbData> d1 = res.getAll(tag1);
      Iterable<CreateData.DumbData> d2 = res.getAll(tag2);
      List<CreateData.DumbData> dataset1 = Lists.newLinkedList(d1);
      final boolean missingD1 = dataset1.size() == 0;
      long count = 0;
      for (CreateData.DumbData ri : d2) {
        count++;
      }
      itemCnt.addValue(count);
      c.output(key.key1 + "," + key.key2 + "," + count);
      if (count == 0) {
        LOG.info("no data2 for (" + key.key1 + "," + key.key2 + ")");
        missD2Cnt.addValue(1L);
      } else if (missingD1) {
        LOG.info(count + " data2 items for (" + key.key1 + "," + key.key2 + ") marked as no-d1");
        missD1Cnt.addValue(1L);
      } else {
        LOG.info(count + " data2 items for (" + key.key1 + "," + key.key2 + ")");
        haveBoth.addValue(1L);
      }
    }
  }

  public static class MergeContainers extends DoFn<KV<Key, Iterable<Container>>, String> {

    private static final Logger LOG = LoggerFactory.getLogger(Merge.class);

    private final Aggregator<Long, Long> missD1Cnt =
        createAggregator("missing data1", new Sum.SumLongFn());

    private final Aggregator<Long, Long> missD2Cnt =
        createAggregator("missing data2", new Sum.SumLongFn());

    private final Aggregator<Long, Long> haveBoth =
        createAggregator("have data from both sources", new Sum.SumLongFn());


    private final Aggregator<Long, Long> itemCnt =
        createAggregator("item count", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) throws Exception {
      KV kv = c.element();
      Key key = c.element().getKey();
      Iterable<Container> containers = c.element().getValue();

      boolean missingD1 = true;
      long count = 0;
      for (Container container : containers) {
        if (container.tag == 1) {
          missingD1 = false;
        } else {
          count++;
        }
      }

      itemCnt.addValue(count);
      c.output(key.key1 + "," + key.key2 + "," + count);
      if (count == 0) {
        LOG.info("no data2 for (" + key.key1 + "," + key.key2 + ")");
        missD2Cnt.addValue(1L);
      } else if (missingD1) {
        LOG.info(count + " data2 items for (" + key.key1 + "," + key.key2 + ") marked as no-d1");
        missD1Cnt.addValue(1L);
      } else {
        LOG.info(count + " data2 items for (" + key.key1 + "," + key.key2 + ")");
        haveBoth.addValue(1L);
      }
    }
  }

  public static class MergeGbk extends DoFn<KV<Key, Iterable<CreateData.DumbData>>, String> {

    private static final Logger LOG = LoggerFactory.getLogger(Merge.class);

    private final Aggregator<Long, Long> keyCnt =
        createAggregator("key count", new Sum.SumLongFn());

    private final Aggregator<Long, Long> itemCnt =
        createAggregator("item count", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) throws Exception {
      KV kv = c.element();
      Key key = c.element().getKey();
      Iterable<CreateData.DumbData> data = c.element().getValue();

      keyCnt.addValue(1L);
      long count = 0;
      for (CreateData.DumbData val : data) {
        count++;
      }
      itemCnt.addValue(count);

      c.output(key.key1 + "," + key.key2 + "," + count);
      if (count == 0) {
        LOG.info("no data for (" + key.key1 + "," + key.key2 + ")");
      } else {
        LOG.info(count + " data items for (" + key.key1 + "," + key.key2 + ")");
      }
    }
  }

  static class KeyedContainer extends SimpleFunction<CreateData.DumbData, KV<Key, Container>> {
    int tag;

    public KeyedContainer(int tag) {
      this.tag = tag;
    }

    @Override
    public KV<Key, Container> apply(CreateData.DumbData dd) {
      return KV.of(new Key(dd.key1, dd.key2), new Container(tag, dd));
    }
  }

  private static final TupleTag<CreateData.DumbData> tag1 = new TupleTag<>();
  private static final TupleTag<CreateData.DumbData> tag2 = new TupleTag<>();

  private enum TestMode {
    COGROUP, CONTAINER, GROUP
  }

  private static final TestMode TEST_MODE = TestMode.GROUP;

  public static void main(String[] args) {
    FlinkPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(FlinkPipelineOptions.class);
//    options.setStreaming(true);
    options.setRunner(FlinkPipelineRunner.class);
    Pipeline p = Pipeline.create(options);

    if (TEST_MODE == TestMode.COGROUP) {
      PCollection<KV<Key, CreateData.DumbData>> dataset1 = p.apply(
          AvroIO.Read.from("/tmp/dataset1-*").withSchema(CreateData.DumbData.class))
          .apply(WithKeys.of(new MakeKey()));

      PCollection<KV<Key, CreateData.DumbData>> dataset2 = p.apply(
          AvroIO.Read.from("/tmp/dataset2-*").withSchema(CreateData.DumbData.class))
          .apply(WithKeys.of(new MakeKey()));

      KeyedPCollectionTuple.of(tag1, dataset1).and(tag2, dataset2)
          .apply(CoGroupByKey.create())
          .apply(ParDo.of(new Merge()))
          .apply(TextIO.Write.named("write data").to("/tmp/test-out"));
    } else if (TEST_MODE == TestMode.CONTAINER) {
      PCollection<KV<Key, Container>> dataset1 = p.apply(
          AvroIO.Read.from("/tmp/dataset1-*").withSchema(CreateData.DumbData.class))
          .apply(MapElements.via(new KeyedContainer(1)));

      PCollection<KV<Key, Container>> dataset2 = p.apply(
          AvroIO.Read.from("/tmp/dataset2-*").withSchema(CreateData.DumbData.class))
          .apply(MapElements.via(new KeyedContainer(2)));
      PCollectionList<KV<Key, Container>> dataList = PCollectionList.of(dataset1).and(dataset2);
      PCollection<KV<Key, Container>> data = dataList.apply(Flatten.pCollections());
      data.apply(GroupByKey.create()).apply(ParDo.of(new MergeContainers()))
          .apply(TextIO.Write.named("write data").to("/tmp/test-out"));
    } else if (TEST_MODE == TestMode.GROUP) {
      PCollection<KV<Key, CreateData.DumbData>> dataset = p.apply(
          AvroIO.Read.from("/tmp/dataset2-*").withSchema(CreateData.DumbData.class))
          .apply(WithKeys.of(new MakeKey()));
      dataset.apply(GroupByKey.create())
          .apply(ParDo.of(new MergeGbk()))
          .apply(TextIO.Write.named("write data").to("/tmp/test-out"));
    }

    p.run();
  }
}
