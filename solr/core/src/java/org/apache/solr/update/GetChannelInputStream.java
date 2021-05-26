package org.apache.solr.update;

import it.unimi.dsi.fastutil.io.MeasurableStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

public class GetChannelInputStream extends InputStream implements MeasurableStream  {
  private final InputStream inputStream;
  private final Channel ch;
  private final int pos;

  public GetChannelInputStream(Channel ch, int pos) {
    this.inputStream = Channels.newInputStream((FileChannel) ch);
    this.ch = ch;
    this.pos = pos;
  }
  
  public Channel getChannel() {
    return ch;
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }

  public void close() throws IOException {
    inputStream.close();
  }

  @Override
  public long length() throws IOException {
    return ((FileChannel)ch).size();
  }

  @Override
  public long position() throws IOException {
    return pos;
  }
}
