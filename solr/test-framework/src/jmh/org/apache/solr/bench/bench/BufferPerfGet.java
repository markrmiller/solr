package org.apache.solr.bench.bench;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsPrepend = {"-Dlog4j.configurationFile=solr/server/resources/log4j2.xml"
        //  "-XX:+FlightRecorder", "-XX:StartFlightRecording=filename=jfr_results/,dumponexit=true,settings=profile,path-to-gc-roots=true"})
})
@Timeout(time = 60)
public class BufferPerfGet {
    private static final byte BYTE = '0';

    @State(Scope.Benchmark)
    public static class BenchState {

        private ByteBuffer byteBuffer;
        private ByteBuffer directByteBuffer;

        private byte[] byteArray;

        private MutableDirectBuffer mutableHeapBuffer;
        private MutableDirectBuffer mutableDirectBuffer;
        private UnsafeBuffer mutableByteArrayBuffer;


        @Setup
        public void setup() {
            byteBuffer = ByteBuffer.allocate(8);
            directByteBuffer = ByteBuffer.allocateDirect(8);

            byteArray = new byte[8];

            mutableHeapBuffer = new UnsafeBuffer(byteBuffer);
            mutableDirectBuffer  = new UnsafeBuffer(directByteBuffer);
            mutableByteArrayBuffer = new UnsafeBuffer(byteArray);

            byteBuffer.put(0, BYTE);
            directByteBuffer.put(0, BYTE);
            byteArray[0] = BYTE;
            mutableHeapBuffer.putByte(0, BYTE);
            mutableDirectBuffer.putByte(0, BYTE);
            mutableByteArrayBuffer.putByte(0, BYTE);
        }
    }


    @Benchmark
    public byte getByteBufferHeap(BenchState state) {
       return state.byteBuffer.get(0);
    }

    @Benchmark
    public byte getByteBufferDirect(BenchState state) {
        return state.directByteBuffer.get(0);
    }

    @Benchmark
    public byte getByteArray(BenchState state) {
        return state.byteArray[0];
    }

    @Benchmark
    public byte getMutableByteBufferHeap(BenchState state) {
        return state.mutableHeapBuffer.getByte(0);
    }

    @Benchmark
    public byte getMutableByteBufferDirect(BenchState state) {
        return state.mutableDirectBuffer.getByte(0);
    }

    @Benchmark
    public byte getMutableByteBufferArray(BenchState state) {
        return state.mutableByteArrayBuffer.getByte(0);
    }
}
