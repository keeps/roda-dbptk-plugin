package org.roda.core.plugins.dbptk;

import java.nio.file.Path;

import org.roda.core.RodaCoreFactory;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class DirectoryManager {
  public static final String WORKSPACE_DIRECTORY = "dbvtk";
  public static final String SOLR_SUBDIRECTORY = "solr";

  public static Path getDataPath() {
    return RodaCoreFactory.getDataPath().resolve("plugin").resolve(WORKSPACE_DIRECTORY);
  }

  public static Path getSolrPidFile(String solrPort) {
    String pidFileName = "solr-" + solrPort + ".pid";
    return getDataPath().resolve(SOLR_SUBDIRECTORY).resolve("bin").resolve(pidFileName);
  }
}
