package org.roda.core.plugins.dbptk;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.roda.core.common.IdUtils;
import org.roda.core.common.iterables.CloseableIterable;
import org.roda.core.data.common.RodaConstants;
import org.roda.core.data.exceptions.InvalidParameterException;
import org.roda.core.data.exceptions.JobException;
import org.roda.core.data.exceptions.RODAException;
import org.roda.core.data.v2.common.OptionalWithCause;
import org.roda.core.data.v2.ip.AIP;
import org.roda.core.data.v2.ip.AIPState;
import org.roda.core.data.v2.ip.DIP;
import org.roda.core.data.v2.ip.File;
import org.roda.core.data.v2.ip.FileLink;
import org.roda.core.data.v2.ip.IndexedFile;
import org.roda.core.data.v2.ip.Representation;
import org.roda.core.data.v2.ip.StoragePath;
import org.roda.core.data.v2.jobs.PluginParameter;
import org.roda.core.data.v2.jobs.PluginType;
import org.roda.core.data.v2.jobs.Report;
import org.roda.core.data.v2.jobs.Report.PluginState;
import org.roda.core.index.IndexService;
import org.roda.core.model.ModelService;
import org.roda.core.model.utils.ModelUtils;
import org.roda.core.plugins.AbstractPlugin;
import org.roda.core.plugins.Plugin;
import org.roda.core.plugins.PluginException;
import org.roda.core.plugins.orchestrate.SimpleJobPluginInfo;
import org.roda.core.plugins.plugins.PluginHelper;
import org.roda.core.storage.DirectResourceAccess;
import org.roda.core.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Main;

public class DatabaseVisualizationPlugin extends AbstractPlugin<AIP> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseVisualizationPlugin.class);

  /**
   * Parameter definition
   */
  private static Map<String, PluginParameter> pluginParameters = new HashMap<>();
  static {
    pluginParameters.put(PluginConstants.PARAMETER_SOLR_HOSTNAME, new PluginParameter(
      PluginConstants.PARAMETER_SOLR_HOSTNAME, "Solr hostname", PluginParameter.PluginParameterType.STRING,
      PluginConstants.getDefaultSolrHostname(), false, false, "The address at which DBVTK Solr can be reached."));
    pluginParameters.put(PluginConstants.PARAMETER_SOLR_PORT, new PluginParameter(PluginConstants.PARAMETER_SOLR_PORT,
      "Solr port", PluginParameter.PluginParameterType.STRING, PluginConstants.getDefaultSolrPort(), false, false,
      "The port at which DBVTK Solr can be reached."));
    pluginParameters.put(PluginConstants.PARAMETER_ZOOKEEPER_HOSTNAME, new PluginParameter(
      PluginConstants.PARAMETER_ZOOKEEPER_HOSTNAME, "Zookeeper hostname", PluginParameter.PluginParameterType.STRING,
      PluginConstants.getDefaultZookeeperHostname(), false, false,
      "The address at which DBVTK Zookeeper can be reached."));
    pluginParameters.put(PluginConstants.PARAMETER_ZOOKEEPER_PORT, new PluginParameter(
      PluginConstants.PARAMETER_ZOOKEEPER_PORT, "Zookeeper port", PluginParameter.PluginParameterType.STRING,
      PluginConstants.getDefaultZookeeperPort(), false, false, "The port at which DBVTK Zookeeper can be reached."));
    pluginParameters.put(PluginConstants.PARAMETER_VISUALIZATION_HOSTNAME, new PluginParameter(
      PluginConstants.PARAMETER_VISUALIZATION_HOSTNAME, "DBVTK hostname", PluginParameter.PluginParameterType.STRING,
      PluginConstants.getDefaultVisualizationHostname(), false, false,
      "The address at which DBVTK web interface can be reached."));
    pluginParameters.put(PluginConstants.PARAMETER_VISUALIZATION_PORT, new PluginParameter(
      PluginConstants.PARAMETER_VISUALIZATION_PORT, "DBVTK port", PluginParameter.PluginParameterType.STRING,
      PluginConstants.getDefaultVisualizationPort(), false, false,
      "The port at which DBVTK web interface can be reached."));
  }

  private String solrHostname;
  private String solrPort;
  private String zookeeperHostname;
  private String zookeeperPort;
  private String visualizationHostname;
  private String visualizationPort;

  @Override
  public String getVersionImpl() {
    return "1.0";
  }

  /**
   * Returns the name of this {@link Plugin}.
   *
   * @return a {@link String} with the name of this {@link Plugin}.
   */
  @Override
  public String getName() {
    return "Database Visualization";
  }

  /**
   * Returns description of this {@link Plugin}.
   *
   * @return a {@link String} with the description of this {@link Plugin}.
   */
  @Override
  public String getDescription() {
    return "Loads SIARD2 databases into a Database Visualization Toolkit instance.";
  }

  /**
   * Returns the type of the execution preservation event linked to this
   * {@link Plugin}.
   *
   * @return a {@link PreservationEventType} with the type of the execution
   *         event of this {@link Plugin}.
   */
  @Override
  public RodaConstants.PreservationEventType getPreservationEventType() {
    return null;
  }

  /**
   * Returns the description of the execution preservation event linked to this
   * {@link Plugin}.
   *
   * @return a {@link String} with the description of the execution event of
   *         this {@link Plugin}.
   */
  @Override
  public String getPreservationEventDescription() {
    return null;
  }

  /**
   * Returns the success message of the execution preservation event linked to
   * this {@link Plugin}.
   *
   * @return a {@link String} with the success message of the execution event of
   *         this {@link Plugin}.
   */
  @Override
  public String getPreservationEventSuccessMessage() {
    return null;
  }

  /**
   * Returns the failure message of the execution preservation event linked to
   * this {@link Plugin}.
   *
   * @return a {@link String} with the failure message of the execution event of
   *         this {@link Plugin}.
   */
  @Override
  public String getPreservationEventFailureMessage() {
    return null;
  }

  /**
   * Method to return Plugin type (so it can be grouped for different purposes)
   */
  @Override
  public PluginType getType() {
    return PluginType.MISC;
  }

  /**
   * Method to return Plugin categories
   */
  @Override
  public List<String> getCategories() {
    return Collections.emptyList();
  }

  /**
   * Method used by PluginManager to obtain a new instance of a plugin, from the
   * current loaded Plugin, to provide to PluginOrchestrator
   */
  @Override
  public Plugin<AIP> cloneMe() {
    return new DatabaseVisualizationPlugin();
  }

  /**
   * Method that validates the parameters provided to the Plugin
   * <p>
   * FIXME this should be changed to return a report
   */
  @Override
  public boolean areParameterValuesValid() {
    return true;
  }

  /**
   * Initializes this {@link Plugin}. This method is called by the
   * {@link PluginManager} before any other methods in the plugin.
   *
   * @throws PluginException
   */
  @Override
  public void init() throws PluginException {
  }

  @Override
  public List<Class<AIP>> getObjectClasses() {
    return Arrays.asList(AIP.class);
  }

  /**
   * Method executed by {@link PluginOrchestrator} before splitting the workload
   * (if it makes sense) by N workers
   *
   * @param index
   * @param model
   * @param storage
   * @throws PluginException
   */
  @Override
  public Report beforeAllExecute(IndexService index, ModelService model, StorageService storage) throws PluginException {
    LOGGER.info("DBVTK-BEFORE-ALL");
    return new Report();
  }

  /**
   * Executes the {@link Plugin}.
   *
   * @param index
   * @param model
   * @param storage
   * @param list
   * @return a {@link Report} of the actions performed.
   * @throws PluginException
   */
  @Override
  public Report execute(IndexService index, ModelService model, StorageService storage, List<AIP> list)
    throws PluginException {

    Report report = PluginHelper.initPluginReport(this);

    try {
      SimpleJobPluginInfo jobPluginInfo = PluginHelper.getInitialJobInformation(this, list.size());
      PluginHelper.updateJobInformation(this, jobPluginInfo);

      for (AIP aip : list) {
        LOGGER.debug("Processing AIP {}", aip.getId());
        Report reportItem = PluginHelper.initPluginReportItem(this, aip.getId(), AIP.class, AIPState.INGEST_PROCESSING);
        PluginHelper.updatePartialJobReport(this, model, index, reportItem, false);
        PluginState pluginResultState = PluginState.SUCCESS;
        PluginState reportState = PluginState.SUCCESS;

        for (Representation representation : aip.getRepresentations()) {
          try {
            LOGGER.debug("Processing representation {} of AIP {}", representation.getId(), aip.getId());

            boolean recursive = true;
            CloseableIterable<OptionalWithCause<File>> allFiles = model.listFilesUnder(aip.getId(),
              representation.getId(), recursive);

            for (OptionalWithCause<File> oFile : allFiles) {
              if (oFile.isPresent()) {
                File file = oFile.get();
                LOGGER.debug("Processing file: {}", file);
                if (!file.isDirectory()) {
                  IndexedFile ifile = index.retrieve(IndexedFile.class, IdUtils.getFileId(file));
                  String fileFormat = ifile.getId().substring(ifile.getId().lastIndexOf('.') + 1,
                    ifile.getId().length());
                  String fileInfoPath = StringUtils.join(
                    Arrays.asList(aip.getId(), representation.getId(), StringUtils.join(file.getPath(), '/'),
                      file.getId()), '/');

                  // FIXME 20161103 bferreira use other means to identify siard2
                  // files
                  if ("siard".equalsIgnoreCase(fileFormat)) {
                    LOGGER.debug("Running veraPDF validator on {}", file.getId());
                    StoragePath fileStoragePath = ModelUtils.getFileStoragePath(file);
                    DirectResourceAccess directAccess = storage.getDirectAccess(fileStoragePath);
                    Path siardPath = directAccess.getPath();
                    DIP dip = new DIP();

                    // FIXME 20161103 bferreira use provided solr/zookeeper
                    // configuration parameters
                    LOGGER.error("path1 {}", siardPath.toAbsolutePath().toString());
                    LOGGER.error("path2 {}", siardPath.toString());
                    int exitStatus = Main.internal_main("-i", "siard-2", "-if", siardPath.toAbsolutePath().toString(),
                      "-e", "solr", "-eh", solrHostname, "-ep", solrPort, "-ezh", zookeeperHostname, "-ezp",
                      zookeeperPort, "-edbid", dip.getId());

                    if (exitStatus == 0) {
                      dip.setOpenExternalURL(getDIPOpenURL(dip));
                      dip.setDeleteExternalURL(getDIPDeleteURL(dip));
                      dip.setDescription("some description for " + dip.getId());
                      dip.setTitle("the title for " + dip.getId());
                      dip.setIsPermanent(false);
                      // TODO: (bferreira) ask lfaria about permissions
                      dip.setPermissions(aip.getPermissions());
                      FileLink fileLink = new FileLink(aip.getId(), representation.getId(), file.getPath(),
                        file.getId());
                      dip.addFile(fileLink);
                      model.createDIP(dip, true);
                    } else {
                      pluginResultState = PluginState.PARTIAL_SUCCESS;
                    }

                    IOUtils.closeQuietly(directAccess);

                    if (!pluginResultState.equals(PluginState.SUCCESS)) {
                      reportItem.addPluginDetails(" Loading into database visualization toolkit failed on "
                        + fileInfoPath.replace("//", "/") + ".");
                    }
                  } else {
                    LOGGER.debug("Ignoring non-siard file: {}", fileInfoPath.replace("//", "/"));
                  }

                }
              } else {
                LOGGER.error("Cannot process AIP representation file", oFile.getCause());
              }
            }

            IOUtils.closeQuietly(allFiles);
            if (!pluginResultState.equals(PluginState.SUCCESS)) {
              reportState = PluginState.FAILURE;
            }
          } catch (RODAException | RuntimeException e) {
            LOGGER.error("Error processing AIP " + aip.getId() + ": " + e.getMessage(), e);
            pluginResultState = PluginState.FAILURE;
            reportState = PluginState.FAILURE;
            reportItem.addPluginDetails(" Loading into database visualization toolkit failed.");
          }
        }

        LOGGER.debug("Creating DBVTK event on AIP {}", aip.getId());

        jobPluginInfo.incrementObjectsProcessed(reportState);
        reportItem.setPluginState(reportState);

        report.addReport(reportItem);
        PluginHelper.updatePartialJobReport(this, model, index, reportItem, true);
        PluginHelper.updateJobInformation(this, jobPluginInfo);
      }

      jobPluginInfo.finalizeInfo();
      PluginHelper.updateJobInformation(this, jobPluginInfo);
    } catch (JobException e) {
      throw new PluginException("A job exception has occurred", e);
    }

    return report;
  }

  /**
   * Method executed by {@link PluginOrchestrator} after all workers have
   * finished their work
   *
   * @param index
   * @param model
   * @param storage
   * @throws PluginException
   */
  @Override
  public Report afterAllExecute(IndexService index, ModelService model, StorageService storage) throws PluginException {
    LOGGER.info("DBVTK-AFTER-ALL");
    return null;
  }

  /**
   * Stops all {@link Plugin} activity. This is the last method to be called by
   * {@link PluginManager} on the {@link Plugin}.
   */
  @Override
  public void shutdown() {

  }

  @Override
  public List<PluginParameter> getParameters() {
    ArrayList<PluginParameter> parameters = new ArrayList<>();
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_SOLR_HOSTNAME));
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_SOLR_PORT));
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_ZOOKEEPER_HOSTNAME));
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_ZOOKEEPER_PORT));
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_HOSTNAME));
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_PORT));
    return parameters;
  }

  @Override
  public void setParameterValues(Map<String, String> parameters) throws InvalidParameterException {
    super.setParameterValues(parameters);
    solrHostname = parameters.get(PluginConstants.PARAMETER_SOLR_HOSTNAME);
    solrPort = parameters.get(PluginConstants.PARAMETER_SOLR_PORT);
    zookeeperHostname = parameters.get(PluginConstants.PARAMETER_ZOOKEEPER_HOSTNAME);
    zookeeperPort = parameters.get(PluginConstants.PARAMETER_ZOOKEEPER_PORT);
    visualizationHostname = parameters.get(PluginConstants.PARAMETER_VISUALIZATION_HOSTNAME);
    visualizationPort = parameters.get(PluginConstants.PARAMETER_VISUALIZATION_PORT);
  }

  private String getDIPOpenURL(DIP dip) {
    return "http://" + visualizationHostname + ":" + visualizationPort + "/index.html#database/" + dip.getId();
  }

  private String getDIPDeleteURL(DIP dip) {
    return "http://" + visualizationHostname + ":" + visualizationPort + "/api/v1/manage/database/" + dip.getId()
      + "/destroy";
  }
}
