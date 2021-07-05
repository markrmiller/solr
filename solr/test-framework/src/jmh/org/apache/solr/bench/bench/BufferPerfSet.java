package org.apache.solr.bench.bench;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@Warmup(time = 5, iterations = 2)
@Measurement(time = 10, iterations = 5)
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

        private MutableDirectBuffer unsafeHeapBuffer;
        private MutableDirectBuffer unsafeDirectBuffer;
        private MutableDirectBuffer unsafeByteArrayBuffer;


        @Setup
        public void setup() {
            byteBuffer = ByteBuffer.allocate(8);
            directByteBuffer = ByteBuffer.allocateDirect(8);

            byteArray = new byte[8];

            unsafeHeapBuffer = new UnsafeBuffer(byteBuffer);
            unsafeDirectBuffer = new UnsafeBuffer(directByteBuffer);
            unsafeByteArrayBuffer = new UnsafeBuffer(byteArray);
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

    @Benchmark
    public MutableDirectBuffer setUnsafeByteBufferHeap(BenchState state) {
        state.unsafeHeapBuffer.putByte(0, state.zeroByte);
        return state.unsafeHeapBuffer;
    }

    @Benchmark
    public MutableDirectBuffer setUnsafeByteBufferDirect(BenchState state) {
        state.unsafeDirectBuffer.putByte(0, state.zeroByte);
        return state.unsafeDirectBuffer;
    }

    @Benchmark
    public MutableDirectBuffer getUnsafeByteBufferArray(BenchState state) {
        state.unsafeByteArrayBuffer.putByte(0, state.zeroByte);
        return state.unsafeByteArrayBuffer;
    }
}
