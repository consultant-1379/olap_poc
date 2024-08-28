package com.ericsson.nms.dg.gen.scratch;

//import com.sun.jdi.Bootstrap;
//import com.sun.jdi.connect.Connector;
//import com.sun.jdi.connect.IllegalConnectorArgumentsException;
//import com.sun.tools.jdi.SocketAttachingConnector;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 11/11/13
 * Time: 14:19
 */
public class Debugger {

  private static final String HOST = "hostname";
  private static final String PORT = "port";

  public static void main(String[] args) throws IOException {
    final InetSocketAddress sa = new InetSocketAddress("10.44.91.78", 6022);
    final Socket s = new Socket();
    s.connect(sa, 10);
    byte[] hello = "1\n".getBytes("UTF-8");
    s.getOutputStream().write(hello);
    final InputStream is = s.getInputStream();
    int read;
    final StringBuilder res = new StringBuilder();
    while ((read = is.read()) != -1) {
      res.append((char) read);
    }
    System.out.println("[" + res.toString() + "]");
    try {
      s.close();
    } catch (Throwable t) {/**/}
  }

 /* private static void attachRemote(final String host, final int port) {
    final SocketAttachingConnector socktAttach = getSocketAttach();
    final Map<String, Connector.Argument> arguments = socktAttach.defaultArguments();
    Connector.Argument hostArg = arguments.get(HOST);
    Connector.Argument portArg = arguments.get(PORT);
    hostArg.setValue(host);
    portArg.setValue(Integer.toString(port));
    try {
      socktAttach.attach(arguments);
    } catch (IOException | IllegalConnectorArgumentsException e) {
      e.printStackTrace();
    }
  }

  private static SocketAttachingConnector getSocketAttach() {
    final String name = "com.sun.jdi.SocketAttach";
    final List<Connector> connectors = Bootstrap.virtualMachineManager().allConnectors();
    for (Connector c : connectors) {
      if (c.name().equals(name)) {
        return (SocketAttachingConnector) c;
      }
    }
    throw new NullPointerException(name);
  }*/
}
