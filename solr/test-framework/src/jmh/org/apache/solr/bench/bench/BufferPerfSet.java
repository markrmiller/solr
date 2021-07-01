package org.apache.solr.bench.bench;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@Warmup(iterations = 4)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsPrepend = {"-Dlog4j.configurationFile=solr/server/resources/log4j2.xml"
        //  "-XX:+FlightRecorder", "-XX:StartFlightRecording=filename=jfr_results/,dumponexit=true,settings=profile,path-to-gc-roots=true"})
})
@Timeout(time = 60)
public class BufferPerfSet {
    private static final byte BYTE = '0';

    @State(Scope.Benchmark)
    public static class BenchState {

        private ByteBuffer byteBuffer;
        private ByteBuffer directByteBuffer;

        private byte[] byteArray;

        private MutableDirectBuffer mutableHeapBuffer;
        private MutableDirectBuffer mutableDirectBuffer;
        private MutableDirectBuffer mutableByteArrayBuffer;


        @Setup
        public void setup() {
            byteBuffer = ByteBuffer.allocate(8);
            directByteBuffer = ByteBuffer.allocateDirect(8);

            byteArray = new byte[8];

            mutableHeapBuffer = new UnsafeBuffer(byteBuffer);
            mutableDirectBuffer  = new UnsafeBuffer(directByteBuffer);
            mutableByteArrayBuffer = new UnsafeBuffer(byteArray);
        }
    }


    @Benchmark
    public void setByteBufferHeap(BenchState state) {
        state.byteBuffer.put(0, BYTE);
    }

    @Benchmark
    public void setByteBufferDirect(BenchState state) {
        state.directByteBuffer.put(0, BYTE);
    }

    @Benchmark
    public void setByteArray(BenchState state) {
        state.byteArray[0] = BYTE;
    }

    @Benchmark
    public void setMutableByteBufferHeap(BenchState state) {
        state.mutableHeapBuffer.putByte(0, BYTE);
    }

    @Benchmark
    public void setMutableByteBufferDirect(BenchState state) {
        state.mutableDirectBuffer.putByte(0, BYTE);
    }

    @Benchmark
    public void getMutableByteBufferArray(BenchState state) {
        state.mutableByteArrayBuffer.putByte(0, BYTE);
    }
}
