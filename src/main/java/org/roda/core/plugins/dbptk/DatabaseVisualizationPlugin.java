package org.roda.core.plugins.dbptk;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.roda.core.common.IdUtils;
import org.roda.core.common.iterables.CloseableIterable;
import org.roda.core.data.common.RodaConstants;
import org.roda.core.data.exceptions.AuthorizationDeniedException;
import org.roda.core.data.exceptions.GenericException;
import org.roda.core.data.exceptions.InvalidParameterException;
import org.roda.core.data.exceptions.JobException;
import org.roda.core.data.exceptions.NotFoundException;
import org.roda.core.data.exceptions.RODAException;
import org.roda.core.data.exceptions.RequestNotValidException;
import org.roda.core.data.v2.IsRODAObject;
import org.roda.core.data.v2.common.OptionalWithCause;
import org.roda.core.data.v2.ip.AIP;
import org.roda.core.data.v2.ip.AIPState;
import org.roda.core.data.v2.ip.DIP;
import org.roda.core.data.v2.ip.File;
import org.roda.core.data.v2.ip.FileLink;
import org.roda.core.data.v2.ip.IndexedAIP;
import org.roda.core.data.v2.ip.IndexedFile;
import org.roda.core.data.v2.ip.Permissions;
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

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.modules.siard.SIARD2ModuleFactory;
import com.databasepreservation.modules.solr.SolrModuleFactory;

public class DatabaseVisualizationPlugin<T extends IsRODAObject> extends AbstractPlugin<T> {
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
    pluginParameters.put(PluginConstants.PARAMETER_VISUALIZATION_OPEN_HOSTNAME, new PluginParameter(
      PluginConstants.PARAMETER_VISUALIZATION_OPEN_HOSTNAME, "DBVTK open hostname",
      PluginParameter.PluginParameterType.STRING, PluginConstants.getDefaultVisualizationOpenHostname(), false, false,
      "The address at which DBVTK web interface can be reached to access a database."));
    pluginParameters.put(PluginConstants.PARAMETER_VISUALIZATION_OPEN_PORT, new PluginParameter(
      PluginConstants.PARAMETER_VISUALIZATION_OPEN_PORT, "DBVTK open port", PluginParameter.PluginParameterType.STRING,
      PluginConstants.getDefaultVisualizationOpenPort(), false, false,
      "The port at which DBVTK web interface can be reached to access a database."));
    pluginParameters.put(PluginConstants.PARAMETER_VISUALIZATION_DELETE_HOSTNAME, new PluginParameter(
      PluginConstants.PARAMETER_VISUALIZATION_DELETE_HOSTNAME, "DBVTK delete hostname",
      PluginParameter.PluginParameterType.STRING, PluginConstants.getDefaultVisualizationDeleteHostname(), false,
      false, "The address at which DBVTK web interface can be reached to delete a database."));
    pluginParameters.put(PluginConstants.PARAMETER_VISUALIZATION_DELETE_PORT, new PluginParameter(
      PluginConstants.PARAMETER_VISUALIZATION_DELETE_PORT, "DBVTK delete port",
      PluginParameter.PluginParameterType.STRING, PluginConstants.getDefaultVisualizationDeletePort(), false, false,
      "The port at which DBVTK web interface can be reached to delete a database."));
  }

  private String solrHostname;
  private String solrPort;
  private String zookeeperHostname;
  private String zookeeperPort;
  private String visualizationOpenHostname;
  private String visualizationOpenPort;
  private String visualizationDeleteHostname;
  private String visualizationDeletePort;

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
  public Plugin<T> cloneMe() {
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
  public List<Class<T>> getObjectClasses() {
    List<Class<? extends IsRODAObject>> list = new ArrayList<>();
    list.add(AIP.class);
    list.add(Representation.class);
    list.add(File.class);
    return (List) list;
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

  @Override
  public Report execute(IndexService index, ModelService model, StorageService storage, List<T> list)
    throws PluginException {

    if (!list.isEmpty()) {
      if (list.get(0) instanceof AIP) {
        return executeOnAIP(index, model, storage, (List<AIP>) list);
      } else if (list.get(0) instanceof Representation) {
        return executeOnRepresentation(index, model, storage, (List<Representation>) list);
      } else if (list.get(0) instanceof File) {
        return executeOnFile(index, model, storage, (List<File>) list);
      }
    }

    return new Report();
  }

  public Report executeOnFile(IndexService index, ModelService model, StorageService storage, List<File> list)
    throws PluginException {

    Report report = PluginHelper.initPluginReport(this);

    try {
      SimpleJobPluginInfo jobPluginInfo = PluginHelper.getInitialJobInformation(this, list.size());
      PluginHelper.updateJobInformation(this, jobPluginInfo);

      for (File file : list) {
        Report reportItem = PluginHelper.initPluginReportItem(this, IdUtils.getFileId(file), File.class,
          AIPState.INGEST_PROCESSING);
        PluginState reportState = PluginState.SUCCESS;

        try {
          List<File> resourceList = new ArrayList<File>();
          PluginHelper.updatePartialJobReport(this, model, index, reportItem, false);
          PluginState pluginResultState = PluginState.SUCCESS;

          LOGGER.debug("Processing file: {}", file);
          if (!file.isDirectory()) {

            IndexedAIP aip = index.retrieve(IndexedAIP.class, file.getAipId());

            IndexedFile ifile = index.retrieve(IndexedFile.class, IdUtils.getFileId(file));
            String fileFormat = ifile.getId().substring(ifile.getId().lastIndexOf('.') + 1, ifile.getId().length());
            String fileInfoPath = StringUtils.join(
              Arrays.asList(file.getAipId(), file.getRepresentationId(), StringUtils.join(file.getPath(), '/'),
                file.getId()), '/');
            pluginResultState = convertToViewer(model, storage, file, reportItem, pluginResultState, fileFormat,
              fileInfoPath, aip.getPermissions());
          }

          if (!pluginResultState.equals(PluginState.SUCCESS)) {
            reportState = PluginState.FAILURE;
          }

          jobPluginInfo.incrementObjectsProcessed(reportState);

        } catch (RequestNotValidException | NotFoundException | GenericException | AuthorizationDeniedException
          | IllegalArgumentException e) {
          jobPluginInfo.incrementObjectsProcessedWithFailure();
          LOGGER.error("Could not run DBPTK successfully");
          reportState = PluginState.FAILURE;
          reportItem.setPluginDetails(e.getMessage());
          jobPluginInfo.incrementObjectsProcessedWithFailure();
        } finally {
          reportItem.setPluginState(reportState);
          report.addReport(reportItem);
          PluginHelper.updatePartialJobReport(this, model, index, reportItem, true);
          PluginHelper.updateJobInformation(this, jobPluginInfo);
        }
      }

      jobPluginInfo.finalizeInfo();
      PluginHelper.updateJobInformation(this, jobPluginInfo);
    } catch (JobException e) {
      throw new PluginException("A job exception has occurred", e);
    }

    return report;
  }

  private PluginState convertToViewer(ModelService model, StorageService storage, File file, Report reportItem,
    PluginState pluginResultState, String fileFormat, String fileInfoPath, Permissions permissions)
    throws RequestNotValidException, GenericException, AuthorizationDeniedException, NotFoundException {
    // FIXME 20161103 bferreira use other means to identify siard2
    if ("siard".equalsIgnoreCase(fileFormat)) {
      LOGGER.debug("Converting {} to the database viewer", file.getId());
      StoragePath fileStoragePath = ModelUtils.getFileStoragePath(file);
      DirectResourceAccess directAccess = storage.getDirectAccess(fileStoragePath);
      Path siardPath = directAccess.getPath();
      DIP dip = new DIP();

      boolean conversionCompleted = convert(siardPath, dip);

      if (conversionCompleted) {
        dip.setType(PluginConstants.DIP_TYPE);
        dip.setDescription("Lightweight web viewer for relational databases, specially if preserved "
          + "in SIARD 2, that uses SOLR as a backend, and allows browsing, search, and export.");
        dip.setTitle("Database Visualization Toolkit");
        dip.setIsPermanent(false);
        dip.setPermissions(permissions);
        FileLink fileLink = new FileLink(file.getAipId(), file.getRepresentationId(), file.getPath(), file.getId());
        dip.addFile(fileLink);
        dip.setProperties(getDipProperties(dip));
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
    return pluginResultState;
  }

  public Report executeOnRepresentation(IndexService index, ModelService model, StorageService storage,
    List<Representation> list) throws PluginException {

    Report report = PluginHelper.initPluginReport(this);

    try {
      SimpleJobPluginInfo jobPluginInfo = PluginHelper.getInitialJobInformation(this, list.size());
      PluginHelper.updateJobInformation(this, jobPluginInfo);

      for (Representation representation : list) {
        Report reportItem = PluginHelper.initPluginReportItem(this, IdUtils.getRepresentationId(representation),
          Representation.class, AIPState.INGEST_PROCESSING);
        PluginHelper.updatePartialJobReport(this, model, index, reportItem, false);
        PluginState reportState = PluginState.SUCCESS;
        StringBuilder details = new StringBuilder();
        AIP aip = model.retrieveAIP(representation.getAipId());

        reportState = internalExecuteOnRepresentation(index, model, storage, aip, reportItem, reportState,
          representation);

        LOGGER.debug("Creating DBVTK event on AIP {}", representation.getAipId());

        jobPluginInfo.incrementObjectsProcessed(reportState);
        reportItem.setPluginState(reportState);

        report.addReport(reportItem);
        PluginHelper.updatePartialJobReport(this, model, index, reportItem, true);
        PluginHelper.updateJobInformation(this, jobPluginInfo);
      }

      jobPluginInfo.finalizeInfo();
      PluginHelper.updateJobInformation(this, jobPluginInfo);
    } catch (JobException | NotFoundException | RequestNotValidException | AuthorizationDeniedException
      | GenericException e) {
      throw new PluginException("A job exception has occurred", e);
    }

    return report;
  }

  public Report executeOnAIP(IndexService index, ModelService model, StorageService storage, List<AIP> list)
    throws PluginException {

    Report report = PluginHelper.initPluginReport(this);

    try {
      SimpleJobPluginInfo jobPluginInfo = PluginHelper.getInitialJobInformation(this, list.size());
      PluginHelper.updateJobInformation(this, jobPluginInfo);

      for (AIP aip : list) {
        LOGGER.debug("Processing AIP {}", aip.getId());
        Report reportItem = PluginHelper.initPluginReportItem(this, aip.getId(), AIP.class, AIPState.INGEST_PROCESSING);
        PluginHelper.updatePartialJobReport(this, model, index, reportItem, false);
        PluginState reportState = PluginState.SUCCESS;

        for (Representation representation : aip.getRepresentations()) {
          reportState = internalExecuteOnRepresentation(index, model, storage, aip, reportItem, reportState,
            representation);
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

  private PluginState internalExecuteOnRepresentation(IndexService index, ModelService model, StorageService storage,
    AIP aip, Report reportItem, PluginState reportState, Representation representation) {
    PluginState pluginResultState = PluginState.SUCCESS;
    try {
      LOGGER.debug("Processing representation {} of AIP {}", representation.getId(), aip.getId());

      boolean recursive = true;
      CloseableIterable<OptionalWithCause<File>> allFiles = model.listFilesUnder(aip.getId(), representation.getId(),
        recursive);

      for (OptionalWithCause<File> oFile : allFiles) {
        if (oFile.isPresent()) {
          File file = oFile.get();
          LOGGER.debug("Processing file: {}", file);
          if (!file.isDirectory()) {
            IndexedFile ifile = index.retrieve(IndexedFile.class, IdUtils.getFileId(file));
            String fileFormat = ifile.getId().substring(ifile.getId().lastIndexOf('.') + 1, ifile.getId().length());
            String fileInfoPath = StringUtils.join(
              Arrays.asList(aip.getId(), representation.getId(), StringUtils.join(file.getPath(), '/'), file.getId()),
              '/');

            convertToViewer(model, storage, file, reportItem, pluginResultState, fileFormat, fileInfoPath,
              aip.getPermissions());

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
    return reportState;
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
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_OPEN_HOSTNAME));
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_OPEN_PORT));
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_DELETE_HOSTNAME));
    parameters.add(pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_DELETE_PORT));
    return parameters;
  }

  @Override
  public void setParameterValues(Map<String, String> parameters) throws InvalidParameterException {
    super.setParameterValues(parameters);
    solrHostname = parameters.get(PluginConstants.PARAMETER_SOLR_HOSTNAME);
    solrPort = parameters.get(PluginConstants.PARAMETER_SOLR_PORT);
    zookeeperHostname = parameters.get(PluginConstants.PARAMETER_ZOOKEEPER_HOSTNAME);
    zookeeperPort = parameters.get(PluginConstants.PARAMETER_ZOOKEEPER_PORT);
    visualizationOpenHostname = parameters.get(PluginConstants.PARAMETER_VISUALIZATION_OPEN_HOSTNAME);
    visualizationOpenPort = parameters.get(PluginConstants.PARAMETER_VISUALIZATION_OPEN_PORT);
    visualizationDeleteHostname = parameters.get(PluginConstants.PARAMETER_VISUALIZATION_DELETE_HOSTNAME);
    visualizationDeletePort = parameters.get(PluginConstants.PARAMETER_VISUALIZATION_DELETE_PORT);
  }

  private Map<String, String> getDipProperties(DIP dip) {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("openHostname", visualizationOpenHostname);
    properties.put("openPort", visualizationOpenPort);
    properties.put("deleteHostname", visualizationDeleteHostname);
    properties.put("deletePort", visualizationDeletePort);
    properties.put("database", dip.getId());
    return properties;
  }

  private boolean convert(Path siardPath, DIP dip) {
    boolean conversionCompleted = false;
    LOGGER.info("starting to convert database " + siardPath.toAbsolutePath().toString());

    // build the SIARD import module
    DatabaseImportModule siardImportModule = null;
    try {
      // create
      DatabaseModuleFactory siardImportFactory = new SIARD2ModuleFactory();
      Map<Parameter, String> siardParameters = new HashMap<>();
      siardParameters.put(siardImportFactory.getAllParameters().get("file"), siardPath.toAbsolutePath().toString());
      siardImportModule = siardImportFactory.buildImportModule(siardParameters);
    } catch (OperationNotSupportedException | LicenseNotAcceptedException e) {
      LOGGER.error("Could not initialize SIARD import module", e);
    }

    // build the Solr export module
    DatabaseExportModule solrExportModule = null;
    try {
      // create
      DatabaseModuleFactory solrExportFactory = new SolrModuleFactory();
      Map<Parameter, String> solrParameters = new HashMap<>();
      solrParameters.put(solrExportFactory.getAllParameters().get("hostname"), solrHostname);
      solrParameters.put(solrExportFactory.getAllParameters().get("port"), solrPort);
      solrParameters.put(solrExportFactory.getAllParameters().get("zookeeper-hostname"), zookeeperHostname);
      solrParameters.put(solrExportFactory.getAllParameters().get("zookeeper-port"), zookeeperPort);
      solrParameters.put(solrExportFactory.getAllParameters().get("database-id"), dip.getId());
      solrExportModule = solrExportFactory.buildExportModule(solrParameters);
    } catch (OperationNotSupportedException | LicenseNotAcceptedException e) {
      LOGGER.error("Could not initialize Solr export module", e);
    }

    if (siardImportModule != null && solrExportModule != null) {
      long startTime = System.currentTimeMillis();
      try {
        siardImportModule.getDatabase(solrExportModule);
        conversionCompleted = true;
      } catch (ModuleException | UnknownTypeException | InvalidDataException e) {
        LOGGER.error("Could not convert the database to the Solr instance.", e);
      }
      long duration = System.currentTimeMillis() - startTime;
      LOGGER.info("Conversion time " + (duration / 60000) + "m " + (duration % 60000 / 1000) + "s");
    }

    return conversionCompleted;
  }
}
