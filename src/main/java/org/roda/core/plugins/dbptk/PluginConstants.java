package org.roda.core.plugins.dbptk;

import org.apache.commons.lang3.StringUtils;
import org.roda.core.RodaCoreFactory;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PluginConstants {
  public static final String PARAMETER_SOLR_HOSTNAME = "core.plugins.dbptk.solr.hostname";
  public static final String PARAMETER_SOLR_PORT = "core.plugins.dbptk.solr.port";
  public static final String PARAMETER_ZOOKEEPER_HOSTNAME = "core.plugins.dbptk.zookeeper.hostname";
  public static final String PARAMETER_ZOOKEEPER_PORT = "core.plugins.dbptk.zookeeper.port";
  public static final String PARAMETER_VISUALIZATION_OPEN_HOSTNAME = "core.plugins.dbptk.visualization.openHostname";
  public static final String PARAMETER_VISUALIZATION_OPEN_PORT = "core.plugins.dbptk.visualization.openPort";
  public static final String PARAMETER_VISUALIZATION_DELETE_HOSTNAME = "core.plugins.dbptk.visualization.deleteHostname";
  public static final String PARAMETER_VISUALIZATION_DELETE_PORT = "core.plugins.dbptk.visualization.deletePort";

  public static final String DIP_TYPE = "dbptk";

  private static final String DEFAULT_SOLR_HOSTNAME = "127.0.0.1";
  private static final String DEFAULT_SOLR_PORT = "8983";
  private static final String DEFAULT_ZOOKEEPER_HOSTNAME = "127.0.0.1";
  private static final String DEFAULT_ZOOKEEPER_PORT = "9983";
  private static final String DEFAULT_VISUALIZATION_OPEN_HOSTNAME = "127.0.0.1";
  private static final String DEFAULT_VISUALIZATION_OPEN_PORT = "9999";
  private static final String DEFAULT_VISUALIZATION_DELETE_HOSTNAME = "127.0.0.1";
  private static final String DEFAULT_VISUALIZATION_DELETE_PORT = "9999";

  public static String getDefaultSolrHostname() {
    String hostname = RodaCoreFactory.getRodaConfigurationAsString(PARAMETER_SOLR_HOSTNAME.split("\\."));
    return StringUtils.isNotBlank(hostname) ? hostname : DEFAULT_SOLR_HOSTNAME;
  }

  public static String getDefaultSolrPort() {
    String port = RodaCoreFactory.getRodaConfigurationAsString(PARAMETER_SOLR_PORT.split("\\."));
    return StringUtils.isNotBlank(port) ? port : DEFAULT_SOLR_PORT;
  }

  public static String getDefaultZookeeperHostname() {
    String hostname = RodaCoreFactory.getRodaConfigurationAsString(PARAMETER_ZOOKEEPER_HOSTNAME.split("\\."));
    return StringUtils.isNotBlank(hostname) ? hostname : DEFAULT_ZOOKEEPER_HOSTNAME;
  }

  public static String getDefaultZookeeperPort() {
    String port = RodaCoreFactory.getRodaConfigurationAsString(PARAMETER_ZOOKEEPER_PORT.split("\\."));
    return StringUtils.isNotBlank(port) ? port : DEFAULT_ZOOKEEPER_PORT;
  }

  public static String getDefaultVisualizationOpenHostname() {
    String hostname = RodaCoreFactory.getRodaConfigurationAsString(PARAMETER_VISUALIZATION_OPEN_HOSTNAME.split("\\."));
    return StringUtils.isNotBlank(hostname) ? hostname : DEFAULT_VISUALIZATION_OPEN_HOSTNAME;
  }

  public static String getDefaultVisualizationOpenPort() {
    String port = RodaCoreFactory.getRodaConfigurationAsString(PARAMETER_VISUALIZATION_OPEN_PORT.split("\\."));
    return StringUtils.isNotBlank(port) ? port : DEFAULT_VISUALIZATION_OPEN_PORT;
  }

  public static String getDefaultVisualizationDeleteHostname() {
    String hostname = RodaCoreFactory
      .getRodaConfigurationAsString(PARAMETER_VISUALIZATION_DELETE_HOSTNAME.split("\\."));
    return StringUtils.isNotBlank(hostname) ? hostname : DEFAULT_VISUALIZATION_DELETE_HOSTNAME;
  }

  public static String getDefaultVisualizationDeletePort() {
    String port = RodaCoreFactory.getRodaConfigurationAsString(PARAMETER_VISUALIZATION_DELETE_PORT.split("\\."));
    return StringUtils.isNotBlank(port) ? port : DEFAULT_VISUALIZATION_DELETE_PORT;
  }
}
