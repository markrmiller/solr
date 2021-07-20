/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.bench.bytebuffers;

import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@Warmup(time = 5, iterations = 2)
@Measurement(time = 10, iterations = 5)
// unless fork=0, this jvm jvmArgsPrepend only applies when running via IDE, when using gradle, these come from build.gradle
@Fork(value = 1, jvmArgsPrepend = {"-Dlog4j.configurationFile=solr/server/resources/log4j2.xml"
        //  "-XX:+FlightRecorder", "-XX:StartFlightRecording=filename=jfr_results/,dumponexit=true,settings=profile,path-to-gc-roots=true"})
})
@Timeout(time = 60)
public class BufferPerfSet {

    @State(Scope.Benchmark)
    public static class BenchState {
        private final byte zeroByte = '0';

        private ByteBuffer byteBuffer;
        private ByteBuffer directByteBuffer;

        private byte[] byteArray;

        @Setup
        public void setup() {
            byteBuffer = ByteBuffer.allocate(8);
            directByteBuffer = ByteBuffer.allocateDirect(8);

            byteArray = new byte[8];
        }
    }

    @Benchmark
    public ByteBuffer setByteBufferHeap(BenchState state) {
        state.byteBuffer.put(0, state.zeroByte);
        return state.byteBuffer;
    }

    @Benchmark
    public ByteBuffer setByteBufferDirect(BenchState state) {
        state.directByteBuffer.put(0, state.zeroByte);
        return state.directByteBuffer;
    }

    @Benchmark
    public byte[] setByteArray(BenchState state) {
        state.byteArray[0] = state.zeroByte;
        return state.byteArray;
    }

}
