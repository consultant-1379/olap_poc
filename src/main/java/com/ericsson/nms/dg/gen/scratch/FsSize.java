package com.ericsson.nms.dg.gen.scratch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: eeipca
 * Date: 12/11/13
 * Time: 09:33
 */
public class FsSize {
  public static void main(String[] args) throws InterruptedException, IOException {
    System.out.println(getFileSystemInfo("/var/datagen/fs_1/rop_files/").get("used_percent"));
    System.out.println(getFileSystemInfo("/var/datagen/fs_2/rop_files/").get("used_percent"));
  }

  public static Map<String, String> getFileSystemInfo(final String fsPath) throws IOException, InterruptedException {
    final List<String> command = Arrays.asList("/bin/df", "-h", "-P", fsPath);
    final ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    final Process p = pb.start();
    final StringBuilder stdout = new StringBuilder();
    String line;
    final BufferedReader processStdout = new BufferedReader(new InputStreamReader(p.getInputStream()));
    while ((line = processStdout.readLine()) != null) {
      line = line.trim();
      if (line.length() == 0) {
        continue;
      }
      stdout.append(line).append("\n");
    }
    final int code = p.waitFor();
    if (code != 0) {
      throw new IOException(stdout.toString().trim());
    }
    final String[] parts = stdout.toString().split("\n")[1].split("\\s+");
    final Map<String, String> info = new HashMap<>();
    info.put("filesystem", parts[0]);
    info.put("size", parts[1]);
    info.put("used_size", parts[2]);
    info.put("available", parts[3]);
    info.put("used_percent", parts[4]);
    info.put("mount_path", parts[5]);
    return info;
  }
}
