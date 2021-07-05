package org.apache.solr.bench.bench;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@Warmup(time = 10, iterations = 2)
@Measurement(time = 5, iterations = 5)
@Fork(value = 1, jvmArgsPrepend = {"-Dlog4j.configurationFile=solr/server/resources/log4j2.xml"
        //  "-XX:+FlightRecorder", "-XX:StartFlightRecording=filename=jfr_results/,dumponexit=true,settings=profile,path-to-gc-roots=true"})
})
@Timeout(time = 60)
public class BufferPerfGet {

    @State(Scope.Benchmark)
    public static class BenchState {

        private final byte zeroByte = '0';

        private ByteBuffer byteBuffer;
        private ByteBuffer directByteBuffer;

        private byte[] byteArray;

        private MutableDirectBuffer unsafeHeapBuffer;
        private MutableDirectBuffer unsafeDirectBuffer;
        private UnsafeBuffer unsafeByteArrayBuffer;


        @Setup
        public void setup() {
            byteBuffer = ByteBuffer.allocate(8);
            directByteBuffer = ByteBuffer.allocateDirect(8);

            byteArray = new byte[8];

            unsafeHeapBuffer = new UnsafeBuffer(byteBuffer);
            unsafeDirectBuffer = new UnsafeBuffer(directByteBuffer);
            unsafeByteArrayBuffer = new UnsafeBuffer(byteArray);

            byteBuffer.put(0, zeroByte);
            directByteBuffer.put(0, zeroByte);
            byteArray[0] = zeroByte;
            unsafeHeapBuffer.putByte(0, zeroByte);
            unsafeDirectBuffer.putByte(0, zeroByte);
            unsafeByteArrayBuffer.putByte(0, zeroByte);
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
    public byte getUnsafeByteBufferHeap(BenchState state) {
        return state.unsafeHeapBuffer.getByte(0);
    }

    @Benchmark
    public byte getUnsafeByteBufferDirect(BenchState state) {
        return state.unsafeDirectBuffer.getByte(0);
    }

    @Benchmark
    public byte getUnsafeByteBufferArray(BenchState state) {
        return state.unsafeByteArrayBuffer.getByte(0);
    }
}
