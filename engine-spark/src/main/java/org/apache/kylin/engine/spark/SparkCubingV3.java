/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.kylin.engine.spark;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.BaseCuboidBuilder;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.NDCuboidBuilder;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import static org.apache.kylin.engine.spark.SparkCubing.getKyroClasses;

/**
 */
public class SparkCubingV3 extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkCubing.class);

    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName("path").hasArg().isRequired(true).withDescription("Hive Intermediate Table").create("hiveTable");
    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg().isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segment").hasArg().isRequired(true).withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_CONF_PATH = OptionBuilder.withArgName("confPath").hasArg().isRequired(true).withDescription("Configuration Path").create("confPath");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg().isRequired(true).withDescription("Cube output path").create(BatchConstants.ARG_OUTPUT);

    private Options options;

    public SparkCubingV3() {
        options = new Options();
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_CONF_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    private void setupClasspath(JavaSparkContext sc, String confPath) throws Exception {
        ClassUtil.addClasspath(confPath);
        final File[] files = new File(confPath).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.getAbsolutePath().endsWith(".xml")) {
                    return true;
                }
                if (pathname.getAbsolutePath().endsWith(".properties")) {
                    return true;
                }
                return false;
            }
        });
        for (File file : files) {
            sc.addFile(file.getAbsolutePath());
        }
    }


    private static final void prepare() {
        final File file = new File(SparkFiles.get("kylin.properties"));
        final String confPath = file.getParentFile().getAbsolutePath();
        System.out.println("conf directory:" + confPath);
        System.setProperty(KylinConfig.KYLIN_CONF, confPath);
        ClassUtil.addClasspath(confPath);
    }


    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        final String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        final String confPath = optionsHelper.getOptionValue(OPTION_CONF_PATH);
        final String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);

        SparkConf conf = new SparkConf().setAppName("Cubing Application");
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired", "true");
        final Iterable<String> allClasses = Iterables.filter(Iterables.concat(Lists.newArrayList(conf.get("spark.kryo.classesToRegister", "").split(",")), getKyroClasses()), new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null && input.trim().length() > 0;
            }
        });
        conf.set("spark.kryo.classesToRegister", StringUtils.join(allClasses, ","));

        JavaSparkContext sc = new JavaSparkContext(conf);
        setupClasspath(sc, confPath);
        HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));

        HiveContext sqlContext = new HiveContext(sc.sc());
        final DataFrame intermediateTable = sqlContext.sql("select * from " + hiveTable);

        System.setProperty(KylinConfig.KYLIN_CONF, confPath);
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        final CubeJoinedFlatTableEnrich intermediateTableDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);

        final Broadcast<CubeDesc> vCubeDesc = sc.broadcast(cubeDesc);
        final Broadcast<CubeSegment> vCubeSegment = sc.broadcast(cubeSegment);
        final Broadcast<BufferedMeasureCodec> vCodec = sc.broadcast(new BufferedMeasureCodec(cubeDesc.getMeasures()));
        NDCuboidBuilder ndCuboidBuilder = new NDCuboidBuilder(vCubeSegment.getValue(), new RowKeyEncoderProvider(cubeSegment));

        final Broadcast<NDCuboidBuilder> vNDCuboidBuilder = sc.broadcast(ndCuboidBuilder);
        final Broadcast<CuboidScheduler> vCuboidScheduler = sc.broadcast(new CuboidScheduler(vCubeDesc.getValue()));

        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        final int measureNum = cubeDesc.getMeasures().size();
        final BaseCuboidBuilder baseCuboidBuilder = new BaseCuboidBuilder(kylinConfig, vCubeDesc.getValue(), vCubeSegment.getValue(), intermediateTableDesc,
                AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid),
                vCodec.getValue(), MeasureIngester.create(cubeDesc.getMeasures()), cubeSegment.buildDictionaryMap());

        boolean[] needAggr = new boolean[cubeDesc.getMeasures().size()];
        boolean allNormalMeasure = true;
        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            needAggr[i] = !cubeDesc.getMeasures().get(i).getFunction().getMeasureType().onlyAggrInBaseCuboid();
            allNormalMeasure = allNormalMeasure && needAggr[i];
        }
        logger.info("All measure are normal (agg on all cuboids) ? : " + allNormalMeasure);

        // encode with dimension encoding, transform to <byte[], Object[]> RDD
        final JavaPairRDD<byte[], Object[]> encodedBaseRDD = intermediateTable.javaRDD().mapToPair(new PairFunction<Row, byte[], Object[]>() {
            @Override
            public Tuple2<byte[], Object[]> call(Row row) throws Exception {
                String[] rowArray = rowToArray(row);
                baseCuboidBuilder.resetAggrs();
                byte[] rowKey = baseCuboidBuilder.buildKey(rowArray);
                Object[] result = baseCuboidBuilder.buildValueObjects(rowArray);
                return new Tuple2<>(rowKey, result);
            }

            private String[] rowToArray(Row row) {
                String[] result = new String[row.size()];
                for (int i = 0; i < row.size(); i++) {
                    final Object o = row.get(i);
                    if (o != null) {
                        result[i] = o.toString();
                    } else {
                        result[i] = null;
                    }
                }
                return result;
            }

        });


        final CuboidReducerFunction2 reducerFunction2 = new CuboidReducerFunction2(measureNum, vCubeDesc.getValue(), vCodec.getValue());
        CuboidReducerFunction2 baseCuboidReducerFunction = reducerFunction2;
        if (allNormalMeasure == false) {
            baseCuboidReducerFunction = new BaseCuboidReducerFunction2(measureNum, vCubeDesc.getValue(), vCodec.getValue(), needAggr);
        }

        // aggregate to calculate base cuboid
        final JavaPairRDD<byte[], Object[]> baseCuboidRDD = encodedBaseRDD.reduceByKey(baseCuboidReducerFunction);
        persistent(baseCuboidRDD, vCodec.getValue(), outputPath, 0, sc.hadoopConfiguration());

        // aggregate to ND cuboids
        final int totalLevels = cubeDesc.getBuildLevel();

        JavaPairRDD<byte[], Object[]> parentRDD = baseCuboidRDD;
        for (int level = 1; level <= totalLevels; level++) {
            JavaPairRDD<byte[], Object[]> childRDD = parentRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<byte[], Object[]>, byte[], Object[]>() {

                transient boolean initialized = false;

                RowKeySplitter rowKeySplitter = new RowKeySplitter(vCubeSegment.getValue(), 65, 256);

                @Override
                public Iterable<Tuple2<byte[], Object[]>> call(Tuple2<byte[], Object[]> tuple2) throws Exception {
                    if (initialized == false) {
                        prepare();
                        initialized = true;
                    }

                    List<Tuple2<byte[], Object[]>> tuples = Lists.newArrayList();
                    byte[] key = tuple2._1();
                    long cuboidId = rowKeySplitter.split(key);
                    Cuboid parentCuboid = Cuboid.findById(vCubeDesc.getValue(), cuboidId);

                    Collection<Long> myChildren = vCuboidScheduler.getValue().getSpanningCuboid(cuboidId);

                    // if still empty or null
                    if (myChildren == null || myChildren.size() == 0) {
                        return tuples;
                    }

                    for (Long child : myChildren) {
                        Cuboid childCuboid = Cuboid.findById(vCubeDesc.getValue(), child);
                        Pair<Integer, ByteArray> result = vNDCuboidBuilder.getValue().buildKey(parentCuboid, childCuboid, rowKeySplitter.getSplitBuffers());

                        byte[] newKey = new byte[result.getFirst()];
                        System.arraycopy(result.getSecond().array(), 0, newKey, 0, result.getFirst());

                        tuples.add(new Tuple2<>(newKey, tuple2._2()));
                    }

                    return tuples;
                }
            }).reduceByKey(reducerFunction2);

            // persistent rdd to hdfs
            persistent(childRDD, vCodec.getValue(), outputPath, level, sc.hadoopConfiguration());
            parentRDD = childRDD;
        }

        logger.info("Finished on calculating all level cuboids.");

    }

    private void persistent(final JavaPairRDD<byte[], Object[]> rdd, final BufferedMeasureCodec codec, final String hdfsBaseLocation, int level, Configuration conf) {
        final String cuboidOutputPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(hdfsBaseLocation, level);
        final JavaPairRDD<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text> serializedRDD = rdd.mapToPair(new PairFunction<Tuple2<byte[], Object[]>, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text>() {
            @Override
            public Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text> call(Tuple2<byte[], Object[]> tuple2) throws Exception {
                ByteBuffer valueBuf = codec.encode(tuple2._2());
                byte[] encodedBytes = new byte[valueBuf.position()];
                System.arraycopy(valueBuf.array(), 0, encodedBytes, 0, valueBuf.position());
                return new Tuple2<>(new org.apache.hadoop.io.Text(tuple2._1()), new org.apache.hadoop.io.Text(encodedBytes));
            }
        });
        logger.debug("Persisting RDD for level " + level + " into " + cuboidOutputPath);
        serializedRDD.saveAsNewAPIHadoopFile(cuboidOutputPath, org.apache.hadoop.io.Text.class, org.apache.hadoop.io.Text.class, SequenceFileOutputFormat.class, conf);
        logger.debug("Done: persisting RDD for level " + level);
    }

    class CuboidReducerFunction2 implements Function2<Object[], Object[], Object[]> {
        BufferedMeasureCodec codec;
        CubeDesc cubeDesc;
        int measureNum;
        transient ThreadLocal<MeasureAggregators> current = new ThreadLocal<>();

        CuboidReducerFunction2(int measureNum, CubeDesc cubeDesc, BufferedMeasureCodec codec) {
            this.codec = codec;
            this.cubeDesc = cubeDesc;
            this.measureNum = measureNum;
        }

        @Override
        public Object[] call(Object[] input1, Object[] input2) throws Exception {
            if (current.get() == null) {
                current.set(new MeasureAggregators(cubeDesc.getMeasures()));
            }
            Object[] result = new Object[measureNum];
            current.get().reset();
            current.get().aggregate(input1);
            current.get().aggregate(input2);
            current.get().collectStates(result);
            return result;
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            current = new ThreadLocal();
        }
    }

    class BaseCuboidReducerFunction2 extends CuboidReducerFunction2 {
        boolean[] needAggr;

        BaseCuboidReducerFunction2(int measureNum, CubeDesc cubeDesc, BufferedMeasureCodec codec, boolean[] needAggr) {
            super(measureNum, cubeDesc, codec);
            this.needAggr = needAggr;
        }

        @Override
        public Object[] call(Object[] input1, Object[] input2) throws Exception {
            if (current.get() == null) {
                current.set(new MeasureAggregators(cubeDesc.getMeasures()));
            }
            current.get().reset();
            Object[] result = new Object[measureNum];
            current.get().aggregate(input1, needAggr);
            current.get().aggregate(input2, needAggr);
            current.get().collectStates(result);
            return result;
        }
    }
}
