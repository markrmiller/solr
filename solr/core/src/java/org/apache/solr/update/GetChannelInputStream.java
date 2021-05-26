package org.apache.solr.update;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channel;

public class GetChannelInputStream extends InputStream {
  private final InputStream inputStream;
  private final Channel ch;

  public GetChannelInputStream(Channel ch, InputStream inputStream) {
    this.inputStream = inputStream;
    this.ch = ch;
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
}
