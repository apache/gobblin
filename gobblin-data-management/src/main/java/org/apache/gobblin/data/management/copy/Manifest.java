package org.apache.gobblin.data.management.copy;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Manifest schema and serDe
 * https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Manifest+based+distcp+runbook
 */
public class Manifest {
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Type CopyableUnitListType = new TypeToken<ArrayList<CopyableUnit>>(){}.getType();


  public final ArrayList<CopyableUnit> _copyableUnits;

  public Manifest() {
    _copyableUnits = new ArrayList<>();
  }

  public Manifest(ArrayList<CopyableUnit> copyableUnits) {
    _copyableUnits = copyableUnits;
  }

  public void add(Manifest.CopyableUnit copyableUnit) {
    _copyableUnits.add(copyableUnit);
  }

  public static class CopyableUnit {
    public final Integer id;
    public final String fileName;
    public final String fileGroup;
    public final Integer fileSizeInBytes;
    public final Integer fileModificationTime;

    public CopyableUnit(Integer id, String fileName, String fileGroup, Integer fileSizeInBytes, Integer fileModificationTime) {
      this.id = id;
      this.fileName = fileName;
      this.fileGroup = fileGroup;
      this.fileSizeInBytes = fileSizeInBytes;
      this.fileModificationTime = fileModificationTime;
    }
  }

  /**
   *
   * @param fs
   * @param path path manifest file location
   * @return
   * @throws IOException
   */
  public static Manifest read(FileSystem fs, Path path) throws IOException {
    JsonReader jsonReader = new JsonReader(new InputStreamReader(fs.open(path), "UTF-8"));
    return new Manifest(GSON.fromJson(jsonReader, CopyableUnitListType));
  }

  /**
   *
   * @param fs
   * @param path path manifest file location
   * @throws IOException
   */
  public void write(FileSystem fs, Path path) throws IOException {
    String outputJson = GSON.toJson(this._copyableUnits, CopyableUnitListType);
    FSDataOutputStream out =  fs.create(path, true);
    out.write(outputJson.getBytes(StandardCharsets.UTF_8));
    out.flush();
    out.close();
  }

  public static ManifestIterator getReadIterator(FileSystem fs, Path path) throws IOException {
    return new ManifestIterator(fs, path);
  }

  public static class ManifestIterator implements Iterator {
    JsonReader reader;

    public ManifestIterator(FileSystem fs, Path path) throws IOException {
      reader = new JsonReader(new InputStreamReader(fs.open(path), "UTF-8"));
      reader.beginArray();
    }

    @Override
    public boolean hasNext() {
      try {
        return reader.hasNext();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    }

    @Override
    public Manifest.CopyableUnit next() {
      return GSON.fromJson(reader, Manifest.CopyableUnit.class);
    }

    public void close() throws IOException {
      if (reader != null) {
        reader.endArray();
        reader.close();
      }
    }
  }
}
