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
public class BufferPerfAllocate {


    @State(Scope.Benchmark)
    public static class BenchState {

        @Setup
        public void setup() {

        }
    }

    @Benchmark
    public ByteBuffer allocateByteBufferHeap(BenchState state) {
        return ByteBuffer.allocate(8);
    }

    @Benchmark
    public ByteBuffer allocateByteBufferDirect(BenchState state) {
        return ByteBuffer.allocateDirect(8);
    }

    @Benchmark
    public byte[] allocateByteArray(BenchState state) {
        return new byte[8];
    }

    @Benchmark
    public UnsafeBuffer allocateUnsafeByteBufferHeap(BenchState state) {
        return new UnsafeBuffer(ByteBuffer.allocate(8));
    }

    @Benchmark
    public UnsafeBuffer allocateUnsafeByteBufferDirect(BenchState state) {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(8));
    }

    @Benchmark
    public UnsafeBuffer allocateUnsafeByteBufferArray(BenchState state) {
        return new UnsafeBuffer(new byte[8]);
    }
}
