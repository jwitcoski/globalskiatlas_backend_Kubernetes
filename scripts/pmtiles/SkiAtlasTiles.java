import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;

/**
 * Build ski atlas overview or resort-detail PMTiles from combined GeoParquet or staged GeoJSON.
 *
 * Run (Java 22+, planetiler.jar on classpath):
 *   java -cp tools/planetiler/planetiler.jar scripts/pmtiles/SkiAtlasTiles.java \
 *     --tileset=overview --input-dir=output/combined \
 *     --analyzed-path=output/pmtiles_staging/overview/ski_areas_analyzed.geoparquet \
 *     --output=output/pmtiles/ski_overview.pmtiles --minzoom=0 --maxzoom=14 --force
 */
public class SkiAtlasTiles implements Profile {

  private final boolean stripOsmTags;
  private final Map<String, String> sourceToLayer;
  private final Map<String, Integer> sourceMinZoom;

  SkiAtlasTiles(boolean stripOsmTags, Map<String, String> sourceToLayer, Map<String, Integer> sourceMinZoom) {
    this.stripOsmTags = stripOsmTags;
    this.sourceToLayer = sourceToLayer;
    this.sourceMinZoom = sourceMinZoom;
  }

  private int minZoomForSource(String source) {
    return sourceMinZoom.getOrDefault(source, 0);
  }

  @Override
  public void processFeature(SourceFeature feature, FeatureCollector features) {
    String source = feature.getSource();
    String layer = sourceToLayer.get(source);
    if (layer == null) {
      return;
    }
    if (!feature.canBePolygon() && !feature.canBeLine() && !feature.isPoint()) {
      return;
    }
    var out = features.anyGeometry(layer).setMinZoom(minZoomForSource(source));
    for (var entry : feature.tags().entrySet()) {
      String key = entry.getKey();
      if (stripOsmTags && "osm".equals(feature.getSource()) && "tags".equals(key)) {
        continue;
      }
      Object value = entry.getValue();
      if (value != null) {
        out.setAttr(key, value);
      }
    }
  }

  @Override
  public String attribution() {
    return "<a href=\"https://www.openstreetmap.org/copyright\">© OpenStreetMap contributors</a>";
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  private static boolean addSource(Planetiler builder, String sourceName, Path path, boolean fromGeojson) {
    if (path == null || !Files.exists(path)) {
      System.err.println("  Skipping missing source " + sourceName + ": " + path);
      return false;
    }
    if (fromGeojson) {
      builder.addGeoJsonSource(sourceName, path);
    } else {
      builder.addParquetSource(sourceName, List.of(path));
    }
    System.out.println("  Source " + sourceName + ": " + path);
    return true;
  }

  public static void main(String[] args) {
    var arguments = Arguments.fromArgs(args);
    String tileset = arguments.getString("tileset", "overview or resort", "overview");
    Path inputDir = arguments.file("input-dir", "combined parquet directory", Path.of("output/combined"));
    Path stagingDir = arguments.file("staging-dir", "geojson staging directory", Path.of("output/pmtiles_staging"));
    Path analyzedPath = arguments.file(
        "analyzed-path",
        "ski_areas_analyzed geoparquet or geojson",
        stagingDir.resolve("overview").resolve("ski_areas_analyzed.geoparquet"));
    Path output = arguments.file(
        "output",
        "output pmtiles path",
        Path.of("output/pmtiles/ski_overview.pmtiles"));
    boolean fromGeojson = arguments.getBoolean("from-geojson", "read geojson from staging instead of parquet", false);
    boolean stripOsmTags = arguments.getBoolean("strip-osm-tags", "drop OSM tags column from osm layer", false);
    int defaultMinZoom = "resort".equals(tileset) ? 12 : 0;
    int defaultMaxZoom = "resort".equals(tileset) ? 15 : 14;
    arguments.getInteger("minzoom", "minimum zoom", defaultMinZoom);
    arguments.getInteger("maxzoom", "maximum zoom", defaultMaxZoom);

    Map<String, String> sourceToLayer = new LinkedHashMap<>();
    var builder = Planetiler.create(arguments);
    int added = 0;

    if ("overview".equals(tileset)) {
      sourceToLayer.put("lifts", "lifts");
      sourceToLayer.put("pistes", "pistes");
      sourceToLayer.put("ski_areas", "ski_areas");
      sourceToLayer.put("ski_areas_analyzed", "ski_areas_analyzed");

      Path overviewStaging = stagingDir.resolve("overview");
      if (fromGeojson) {
        added += addSource(builder, "lifts", overviewStaging.resolve("lifts.geojson"), true) ? 1 : 0;
        added += addSource(builder, "pistes", overviewStaging.resolve("pistes.geojson"), true) ? 1 : 0;
        added += addSource(builder, "ski_areas", overviewStaging.resolve("ski_areas.geojson"), true) ? 1 : 0;
        added += addSource(builder, "ski_areas_analyzed", overviewStaging.resolve("ski_areas_analyzed.geojson"), true) ? 1 : 0;
      } else {
        added += addSource(builder, "lifts", overviewStaging.resolve("lifts.parquet"), false) ? 1 : 0;
        added += addSource(builder, "pistes", overviewStaging.resolve("pistes.parquet"), false) ? 1 : 0;
        added += addSource(builder, "ski_areas", overviewStaging.resolve("ski_areas.parquet"), false) ? 1 : 0;
        Path analyzedGeojson = analyzedPath.resolveSibling("ski_areas_analyzed.geojson");
        Path analyzed = Files.exists(analyzedPath) ? analyzedPath
            : (Files.exists(analyzedGeojson) ? analyzedGeojson : analyzedPath);
        added += addSource(
            builder,
            "ski_areas_analyzed",
            analyzed,
            analyzed.toString().endsWith(".geojson")) ? 1 : 0;
      }
    } else if ("resort".equals(tileset)) {
      sourceToLayer.put("osm", "osm");
      sourceToLayer.put("buffer", "buffer");
      sourceToLayer.put("contours", "contours");

      Path resortStaging = stagingDir.resolve("resort");
      if (fromGeojson) {
        added += addSource(builder, "osm", resortStaging.resolve("osm_near_winter_sports.geojson"), true) ? 1 : 0;
        added += addSource(builder, "buffer", resortStaging.resolve("ski_areas_1000ft_buffer.geojson"), true) ? 1 : 0;
        added += addSource(builder, "contours", resortStaging.resolve("ski_area_contours.geojson"), true) ? 1 : 0;
      } else {
        added += addSource(builder, "osm", resortStaging.resolve("osm.parquet"), false) ? 1 : 0;
        added += addSource(builder, "buffer", resortStaging.resolve("buffer.parquet"), false) ? 1 : 0;
        added += addSource(builder, "contours", resortStaging.resolve("contours.parquet"), false) ? 1 : 0;
      }
    } else {
      throw new IllegalArgumentException("tileset must be overview or resort, got: " + tileset);
    }

    if (added == 0) {
      throw new IllegalStateException("No input sources found for tileset=" + tileset);
    }

    Map<String, Integer> sourceMinZoom = new LinkedHashMap<>();
    if ("overview".equals(tileset)) {
      // Resort centroid points at world zoom; geometry layers appear as you zoom in.
      sourceMinZoom.put("ski_areas_analyzed", arguments.getInteger(
          "analyzed-min-zoom", "minimum zoom for ski_areas_analyzed points", 0));
      sourceMinZoom.put("ski_areas", arguments.getInteger(
          "ski-areas-min-zoom", "minimum zoom for ski_areas polygons", 8));
      sourceMinZoom.put("pistes", arguments.getInteger(
          "pistes-min-zoom", "minimum zoom for pistes lines/polygons/points", 10));
      sourceMinZoom.put("lifts", arguments.getInteger(
          "lifts-min-zoom", "minimum zoom for lifts", 10));
    } else {
      sourceMinZoom.put("buffer", arguments.getInteger(
          "buffer-min-zoom", "minimum zoom for ski area buffers", 12));
      sourceMinZoom.put("osm", arguments.getInteger(
          "osm-min-zoom", "minimum zoom for nearby OSM features", 12));
      sourceMinZoom.put("contours", arguments.getInteger(
          "contours-min-zoom", "minimum zoom for elevation contours", 13));
    }

    builder
        .setProfile(new SkiAtlasTiles(stripOsmTags, sourceToLayer, sourceMinZoom))
        .overwriteOutput(output)
        .run();
  }
}
