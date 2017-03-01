package org.roda.core.plugins.dbptk;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.roda.core.common.IdUtils;
import org.roda.core.common.iterables.CloseableIterable;
import org.roda.core.data.common.RodaConstants;
import org.roda.core.data.exceptions.AuthorizationDeniedException;
import org.roda.core.data.exceptions.GenericException;
import org.roda.core.data.exceptions.InvalidParameterException;
import org.roda.core.data.exceptions.NotFoundException;
import org.roda.core.data.exceptions.RODAException;
import org.roda.core.data.exceptions.RequestNotValidException;
import org.roda.core.data.v2.IsRODAObject;
import org.roda.core.data.v2.common.OptionalWithCause;
import org.roda.core.data.v2.ip.AIP;
import org.roda.core.data.v2.ip.DIP;
import org.roda.core.data.v2.ip.File;
import org.roda.core.data.v2.ip.FileLink;
import org.roda.core.data.v2.ip.Permissions;
import org.roda.core.data.v2.ip.Representation;
import org.roda.core.data.v2.ip.StoragePath;
import org.roda.core.data.v2.jobs.Job;
import org.roda.core.data.v2.jobs.PluginParameter;
import org.roda.core.data.v2.jobs.PluginType;
import org.roda.core.data.v2.jobs.Report;
import org.roda.core.data.v2.jobs.Report.PluginState;
import org.roda.core.data.v2.validation.ValidationIssue;
import org.roda.core.data.v2.validation.ValidationReport;
import org.roda.core.index.IndexService;
import org.roda.core.model.ModelService;
import org.roda.core.model.utils.ModelUtils;
import org.roda.core.plugins.AbstractAIPComponentsPlugin;
import org.roda.core.plugins.Plugin;
import org.roda.core.plugins.PluginException;
import org.roda.core.plugins.orchestrate.SimpleJobPluginInfo;
import org.roda.core.plugins.plugins.PluginHelper;
import org.roda.core.storage.DirectResourceAccess;
import org.roda.core.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.modules.siard.SIARD2ModuleFactory;
import com.databasepreservation.modules.solr.SolrModuleFactory;

public class DatabaseVisualizationPlugin<T extends IsRODAObject> extends AbstractAIPComponentsPlugin<T> {
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

    pluginParameters.put(PluginConstants.PARAMETER_SIARD_EXTENSIONS, new PluginParameter(
      PluginConstants.PARAMETER_SIARD_EXTENSIONS, "File extensions to process",
      PluginParameter.PluginParameterType.STRING, PluginConstants.getDefaultSiardExtensions(), true, false,
      "The comma-separated list of file extensions that should be considered SIARDs by this task."));

    pluginParameters.put(PluginConstants.PARAMETER_IGNORE_NON_SIARD, new PluginParameter(
      PluginConstants.PARAMETER_IGNORE_NON_SIARD, "Ignore non SIARD files",
      PluginParameter.PluginParameterType.BOOLEAN, PluginConstants.getDefaultSiardIgnoreNonSiard(), false, false,
      "Ignore files that are not identified as SIARD."));
  }

  private String solrHostname;
  private String solrPort;
  private String zookeeperHostname;
  private String zookeeperPort;
  private String visualizationOpenHostname;
  private String visualizationOpenPort;
  private String visualizationDeleteHostname;
  private String visualizationDeletePort;
  private boolean ignoreFiles = Boolean.valueOf(PluginConstants.getDefaultSiardIgnoreNonSiard());
  private List<String> siardExtensions;

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
    return "Uses the Database Preservation Toolkit to load SIARD 2 files into a Database Visualization Toolkit "
      + "instance. This task assumes that the necessary configuration options have been set "
      + "(in “roda-core.properties”), to allow the plugin to communicate with the Database Visualization Toolkit "
      + "instance. The loaded databases are non-volatile, but deleting the SIARD file DIP will also remove the corresponding "
      + "database from the Database Visualization Toolkit.";
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
    return RodaConstants.PreservationEventType.MIGRATION;
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
    return "Create a dissemination for a siard database.";
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
    return Arrays.asList(RodaConstants.PLUGIN_CATEGORY_DISSEMINATION);
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

  protected Report executeOnFile(IndexService index, ModelService model, StorageService storage, Report report,
    SimpleJobPluginInfo jobPluginInfo, List<File> list, Job job) throws PluginException {
    for (File file : list) {
      ValidationReport validationReport = new ValidationReport();
      validationReport.setValid(false);
      Report reportItem = PluginHelper.initPluginReportItem(this, IdUtils.getFileId(file), File.class).setDateCreated(
        new Date());
      PluginState reportState = PluginState.SUCCESS;

      String fileInfoPath = null;
      try {
        PluginHelper.updatePartialJobReport(this, model, index, reportItem, false, job);
        LOGGER.debug("Processing file: {}", file);
        if (!file.isDirectory()) {
          AIP aip = model.retrieveAIP(file.getAipId());
          String fileFormat = file.getId().substring(file.getId().lastIndexOf('.') + 1, file.getId().length());
          fileInfoPath = ModelUtils.getFileStoragePath(file.getAipId(), file.getRepresentationId(), file.getPath(),
            file.getId()).toString();
          PluginState pluginState = convertToViewer(model, storage, file, validationReport, fileFormat, fileInfoPath,
            aip.getPermissions());
          if (pluginState.equals(PluginState.FAILURE)) {
            reportState = PluginState.FAILURE;
          }
        }
        jobPluginInfo.incrementObjectsProcessed(reportState);

      } catch (RequestNotValidException | NotFoundException | GenericException | AuthorizationDeniedException
        | IllegalArgumentException e) {
        jobPluginInfo.incrementObjectsProcessedWithFailure();
        addExceptionToValidationReport(validationReport, "Could not run DBPTK successfully", e);
        reportState = PluginState.FAILURE;
        reportItem.setPluginDetails(e.getMessage());
        jobPluginInfo.incrementObjectsProcessedWithFailure();
      } finally {
        reportItem.setPluginState(reportState);
        if (fileInfoPath != null) {
          addValidationReportAsHtml(validationReport, reportItem,
            "Error list for file " + fileInfoPath.replace("//", "/"));
        } else {
          addValidationReportAsHtml(validationReport, reportItem, "Error list for file " + file.getId()
            + " in Representation " + file.getRepresentationId());
        }
        report.addReport(reportItem);
        PluginHelper.updatePartialJobReport(this, model, index, reportItem, true, job);
      }
    }

    return report;
  }

  private PluginState convertToViewer(ModelService model, StorageService storage, File file,
    ValidationReport validationReport, String fileFormat, String fileInfoPath, Permissions permissions)
    throws RequestNotValidException, GenericException, AuthorizationDeniedException, NotFoundException {
    PluginState pluginResultState = PluginState.SUCCESS;

    // FIXME 20161103 bferreira use other means to identify siard2
    if (siardExtensions.contains(fileFormat)) {
      LOGGER.debug("Converting {} to the database viewer", file.getId());
      StoragePath fileStoragePath = ModelUtils.getFileStoragePath(file);
      DirectResourceAccess directAccess = storage.getDirectAccess(fileStoragePath);
      Path siardPath = directAccess.getPath();
      DIP dip = new DIP();

      boolean conversionCompleted = convert(siardPath, dip, validationReport);

      if (conversionCompleted) {
        dip.setType(PluginConstants.DIP_TYPE);
        dip.setDescription("Lightweight web viewer for relational databases. It allows browsing, search and export.");
        dip.setTitle("Database Visualization Toolkit");
        dip.setIsPermanent(false);
        dip.setPermissions(permissions);
        FileLink fileLink = new FileLink(file.getAipId(), file.getRepresentationId(), file.getPath(), file.getId());
        dip.addFile(fileLink);
        dip.setProperties(getDipProperties(dip));
        model.createDIP(dip, true);
      } else {
        pluginResultState = PluginState.FAILURE;
      }

      IOUtils.closeQuietly(directAccess);

      if (!pluginResultState.equals(PluginState.SUCCESS)) {
        addMessageToValidationReport(validationReport, "Loading into database visualization toolkit failed on "
          + fileInfoPath.replace("//", "/") + ".");
      }
    } else {
      if (ignoreFiles) {
        addMessageToValidationReport(validationReport, "Ignoring non-siard file: " + fileInfoPath.replace("//", "/"));
      } else {
        addMessageToValidationReport(validationReport, "Found non-siard file: " + fileInfoPath.replace("//", "/"));
        pluginResultState = PluginState.FAILURE;
      }
    }
    return pluginResultState;
  }

  protected Report executeOnRepresentation(IndexService index, ModelService model, StorageService storage,
    Report report, SimpleJobPluginInfo jobPluginInfo, List<Representation> list, Job job) throws PluginException {
    for (Representation representation : list) {
      ValidationReport validationReport = new ValidationReport();
      validationReport.setValid(false);
      Report reportItem = PluginHelper.initPluginReportItem(this, IdUtils.getRepresentationId(representation),
        Representation.class).setDateCreated(new Date());
      PluginHelper.updatePartialJobReport(this, model, index, reportItem, false, job);
      PluginState reportState = PluginState.SUCCESS;

      try {
        AIP aip = model.retrieveAIP(representation.getAipId());
        LOGGER.debug("Creating DBVTK event on AIP {}", representation.getAipId());
        reportState = internalExecuteOnRepresentation(index, model, storage, aip, reportItem, reportState,
          representation);
      } catch (RequestNotValidException | NotFoundException | GenericException | AuthorizationDeniedException e) {
        addExceptionToValidationReport(validationReport, "Could not retrieve AIP for representation", e);
        reportState = PluginState.FAILURE;
      }

      jobPluginInfo.incrementObjectsProcessed(reportState);
      reportItem.setPluginState(reportState);
      addValidationReportAsHtml(validationReport, reportItem, "Error list for AIP " + representation.getAipId());
      report.addReport(reportItem);

      PluginHelper.updatePartialJobReport(this, model, index, reportItem, true, job);
    }

    return report;
  }

  protected Report executeOnAIP(IndexService index, ModelService model, StorageService storage, Report report,
    SimpleJobPluginInfo jobPluginInfo, List<AIP> list, Job job) throws PluginException {
    for (AIP aip : list) {
      LOGGER.debug("Processing AIP {}", aip.getId());
      Report reportItem = PluginHelper.initPluginReportItem(this, aip.getId(), AIP.class).setDateCreated(new Date());
      PluginHelper.updatePartialJobReport(this, model, index, reportItem, false, job);
      PluginState reportState = PluginState.SUCCESS;

      for (Representation representation : aip.getRepresentations()) {
        reportState = internalExecuteOnRepresentation(index, model, storage, aip, reportItem, reportState,
          representation);
      }

      LOGGER.debug("Creating DBVTK event on AIP {}", aip.getId());

      jobPluginInfo.incrementObjectsProcessed(reportState);
      reportItem.setPluginState(reportState);

      report.addReport(reportItem);
      PluginHelper.updatePartialJobReport(this, model, index, reportItem, true, job);
    }

    return report;
  }

  private PluginState internalExecuteOnRepresentation(IndexService index, ModelService model, StorageService storage,
    AIP aip, Report reportItem, PluginState reportState, Representation representation) {

    CloseableIterable<OptionalWithCause<File>> allFiles = null;
    ValidationReport representationValidationReport = new ValidationReport();
    try {
      LOGGER.debug("Processing representation {} of AIP {}", representation.getId(), aip.getId());

      boolean recursive = true;
      allFiles = model.listFilesUnder(aip.getId(), representation.getId(), recursive);

      for (OptionalWithCause<File> oFile : allFiles) {
        if (oFile.isPresent()) {
          File file = oFile.get();
          LOGGER.debug("Processing file: {}", file);
          if (!file.isDirectory()) {
            String fileFormat = file.getId().substring(file.getId().lastIndexOf('.') + 1, file.getId().length());
            String fileInfoPath = ModelUtils.getFileStoragePath(aip.getId(), representation.getId(), file.getPath(),
              file.getId()).toString();

            ValidationReport validationReport = new ValidationReport();
            validationReport.setValid(false);
            PluginState pluginState = convertToViewer(model, storage, file, validationReport, fileFormat, fileInfoPath,
              aip.getPermissions());
            addValidationReportAsHtml(validationReport, reportItem,
              "Error list for file " + fileInfoPath.replace("//", "/"));

            if (pluginState.equals(PluginState.FAILURE)) {
              reportState = PluginState.FAILURE;
            }
          }
        } else {
          addExceptionToValidationReport(representationValidationReport, "Cannot process file", oFile.getCause());
        }
      }
    } catch (RODAException | RuntimeException e) {
      addExceptionToValidationReport(representationValidationReport, "Error processing AIP " + aip.getId(), e);
      reportState = PluginState.FAILURE;
    } finally {
      IOUtils.closeQuietly(allFiles);
    }
    addValidationReportAsHtml(representationValidationReport, reportItem, "Error list for Representation "
      + representation.getId());
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
    return Arrays.asList(pluginParameters.get(PluginConstants.PARAMETER_SIARD_EXTENSIONS),
      pluginParameters.get(PluginConstants.PARAMETER_IGNORE_NON_SIARD));
  }

  @Override
  public void setParameterValues(Map<String, String> parameters) throws InvalidParameterException {
    super.setParameterValues(parameters);

    // get the values (or defaults) for these
    String siardExtensionsString = parameters.get(PluginConstants.PARAMETER_SIARD_EXTENSIONS);
    siardExtensions = Arrays.asList(siardExtensionsString.split(","));

    ignoreFiles = Boolean.valueOf(parameters.get(PluginConstants.PARAMETER_IGNORE_NON_SIARD));

    // use defaults for these
    solrHostname = pluginParameters.get(PluginConstants.PARAMETER_SOLR_HOSTNAME).getDefaultValue();
    solrPort = pluginParameters.get(PluginConstants.PARAMETER_SOLR_PORT).getDefaultValue();
    zookeeperHostname = pluginParameters.get(PluginConstants.PARAMETER_ZOOKEEPER_HOSTNAME).getDefaultValue();
    zookeeperPort = pluginParameters.get(PluginConstants.PARAMETER_ZOOKEEPER_PORT).getDefaultValue();
    visualizationOpenHostname = pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_OPEN_HOSTNAME)
      .getDefaultValue();
    visualizationOpenPort = pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_OPEN_PORT).getDefaultValue();
    visualizationDeleteHostname = pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_DELETE_HOSTNAME)
      .getDefaultValue();
    visualizationDeletePort = pluginParameters.get(PluginConstants.PARAMETER_VISUALIZATION_DELETE_PORT)
      .getDefaultValue();
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

  private boolean convert(Path siardPath, DIP dip, ValidationReport validationReport) {
    boolean conversionCompleted = false;
    LOGGER.info("starting to convert database " + siardPath.toAbsolutePath().toString());

    // build the SIARD import module, Solr export module, and start the
    // conversion
    try {
      Reporter reporter = new Reporter(PluginHelper.getJobWorkingDirectory(this).toAbsolutePath().toString());

      DatabaseModuleFactory siardImportFactory = new SIARD2ModuleFactory(reporter);
      Map<Parameter, String> siardParameters = new HashMap<>();
      siardParameters.put(siardImportFactory.getAllParameters().get("file"), siardPath.toAbsolutePath().toString());
      DatabaseImportModule siardImportModule = siardImportFactory.buildImportModule(siardParameters);
      siardImportModule.setOnceReporter(reporter);

      DatabaseModuleFactory solrExportFactory = new SolrModuleFactory(reporter);
      Map<Parameter, String> solrParameters = new HashMap<>();
      solrParameters.put(solrExportFactory.getAllParameters().get("hostname"), solrHostname);
      solrParameters.put(solrExportFactory.getAllParameters().get("port"), solrPort);
      solrParameters.put(solrExportFactory.getAllParameters().get("zookeeper-hostname"), zookeeperHostname);
      solrParameters.put(solrExportFactory.getAllParameters().get("zookeeper-port"), zookeeperPort);
      solrParameters.put(solrExportFactory.getAllParameters().get("database-id"), dip.getId());
      DatabaseExportModule solrExportModule = solrExportFactory.buildExportModule(solrParameters);
      solrExportModule.setOnceReporter(reporter);

      long startTime = System.currentTimeMillis();
      try {
        siardImportModule.getDatabase(solrExportModule);
        conversionCompleted = true;
      } catch (ModuleException | UnknownTypeException | RuntimeException e) {
        addExceptionToValidationReport(validationReport, "Could not convert the database to the Solr instance.", e);
      }
      long duration = System.currentTimeMillis() - startTime;
      LOGGER.info("Conversion time " + (duration / 60000) + "m " + (duration % 60000 / 1000) + "s");
    } catch (ModuleException e) {
      addExceptionToValidationReport(validationReport, "Could not initialize modules", e);
    }

    return conversionCompleted;
  }

  private void addExceptionToValidationReport(ValidationReport validationreport, String message, Exception exception) {
    LOGGER.error(message, exception);
    StringBuilder builder = new StringBuilder(message).append(" details:\n");
    for (Throwable throwable : ExceptionUtils.getThrowables(exception)) {
      builder.append(ExceptionUtils.getMessage(throwable)).append("\n");
    }
    validationreport.addIssue(new ValidationIssue(builder.toString()));
  }

  private void addMessageToValidationReport(ValidationReport validationreport, String message) {
    LOGGER.info(message);
    validationreport.addIssue(new ValidationIssue(message));
  }

  private void addValidationReportAsHtml(ValidationReport validationReport, Report reportItem, String title) {
    if (!validationReport.getIssues().isEmpty()) {
      reportItem.setHtmlPluginDetails(true).addPluginDetails(validationReport.toHtml(false, false, false, title));
    }
  }
}
