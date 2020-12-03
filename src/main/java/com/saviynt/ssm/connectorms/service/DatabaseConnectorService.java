package com.saviynt.ssm.connectorms.service;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saviynt.ssm.abstractConnector.BaseConnectorSpecification;
import com.saviynt.ssm.abstractConnector.RepositoryReconService;
import com.saviynt.ssm.abstractConnector.SearchableObject;
import com.saviynt.ssm.abstractConnector.exceptions.ConnectorException;
import com.saviynt.ssm.abstractConnector.exceptions.InvalidAttributeValueException;
import com.saviynt.ssm.abstractConnector.exceptions.InvalidCredentialException;
import com.saviynt.ssm.abstractConnector.exceptions.MissingKeyException;
import com.saviynt.ssm.abstractConnector.exceptions.OperationTimeoutException;
import com.saviynt.ssm.abstractConnector.utility.GroovyService;

public class DatabaseConnectorService extends BaseConnectorSpecification {
	private static final long serialVersionUID = 1L;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public String displayName() {

		return "DATABASE";
	}

	@Override
	public String version() {
		return "2.0";
	}
	/**
	 * to test the connection
	 * Eg : To test the connection , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to target system using JDBC connection
	 * step 3 : return true if connection is successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 * @throws InvalidCredentialException the invalid credential exception
	 * @throws InvalidAttributeValueException the invalid attribute value exception
	 * @throws OperationTimeoutException the operation timeout exception
	 * @throws MissingKeyException the missing key exception
	 */
	@Override
	public Boolean test(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException,
			InvalidCredentialException, InvalidAttributeValueException, OperationTimeoutException, MissingKeyException {
		return true;

	}
	/**
	 * to process reconcile for users and accounts
	 * Example : to process reconcile for users and accounts , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : collect the data(Account,Users,Entitlements) from target system
	 * step 3 : set the data into the format accepted by connector framework's notify
	 * step 4 : call connector framework's notify method 
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
     * @param formatterClass the formatter class
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void reconcile(Map<String, Object> configData, Map<String, Object> data, String formatterClass)
			throws ConnectorException {
		if (data.get("IMPORTABLE_OBJECT") != null && data.get("IMPORTABLE_OBJECT").equals("ACCOUNT")) {
			accountReconcile(configData, data);
			return;
		}
		logger.debug("Enter DatabaseConnectorService reconcile");
		List<List<Map<String, Object>>> finalData = new ArrayList<List<Map<String, Object>>>();
		List<Map<String, Object>> finalDataList = new ArrayList<Map<String, Object>>();
		Connection con = null;

		try {
			JSONObject jsonObject = new JSONObject(data.get("UserReconJSON").toString());
			Map<String, Object> filterData = jsonObject.toMap();

			con = getConnection(configData);
			if (filterData.containsKey("query")) {
				Map<String, Object> mapperMap = (Map<String, Object>) filterData.get("mapper");
				List<Map<String, String>> mapfieldMap = (List<Map<String, String>>) mapperMap.get("mapfield");

				Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(batchSize());

				String query = filterData.get("query").toString();
				ResultSet rs = stmt.executeQuery(query);
				ResultSetMetaData rsmd = rs.getMetaData();
				while (rs.next()) {
					Map<String, Object> dataCol = new HashMap<String, Object>();
					for (int i = 1; i <= rsmd.getColumnCount(); i++) {
						String column = rsmd.getColumnLabel(i);
						dataCol.putIfAbsent(column, rs.getString(i));
					}
					Map<String, Object> mapColumnsData = new HashMap<String, Object>();
					for (Map<String, String> columnPropertyMap : mapfieldMap) {
						if (columnPropertyMap.get("saviyntproperty") != null
								&& columnPropertyMap.get("sourceproperty") != null) {
							mapColumnsData.put(
									("users." + columnPropertyMap.get("saviyntproperty")).trim().toUpperCase(),
									GroovyService.convertTemplateToString(
											columnPropertyMap.get("sourceproperty").trim(), dataCol));
						}

					}
					finalDataList.add(mapColumnsData);

				}

			}
			finalData.add(finalDataList);
			logger.debug("DataSet to save or update = " + finalData);

			RepositoryReconService.notify(finalData, null, formatterClass, data);
			logger.info("End DatabaseConnectorService reconcile");

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ConnectorException(e);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
					throw new ConnectorException(e);
				}
			}

		}

	}

	/**
	 * getConnection
	 * 
	 * @param configData
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private static Connection getConnection(Map<String, Object> configData)
			throws ClassNotFoundException, SQLException {

		Class.forName(configData.get("drivername").toString());
		return DriverManager.getConnection(configData.get("url").toString(), configData.get("username").toString(),
				configData.get("password").toString());

	}
	/**
	 * to check existing record for the input object.
	 * Example : to check existing record for the input object(for account) , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : set the data with filters if any
	 * step 3 : call getObjectList
	 * step 4 : return true if object exists
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean checkExisting(Map<String, Object> configData, Map<String, Object> data,
			SearchableObject serachableObject) throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}
	/**
	 * to create account in the target system 
	 * Example : to create account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to create account in the target system
	 * step 4 : return true if successful
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean createAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService createAccount");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "CreateAccountJSON");
			logger.info("End DatabaseConnectorService createAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount > 0 ? true : false;
	}
	/**
	 * to update account in the target system
	 * Example : to update account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to update account in the target system
	 * step 4 : return the number of records updated
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the integer number of accounts updated
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Integer updateAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService updateAccount");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "UpdateAccountJSON");
			logger.info("End DatabaseConnectorService updateAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount;
	}
	/**
	 * to lock the account in target system
	 * Example : to lock account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to lock account in the target system
	 * step 4 : return true if successful
	 * 
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean lockAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService lockAccount");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "LockAccountJSON");
			logger.info("End DatabaseConnectorService lockAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount > 0 ? true : false;
	}
	/**
	 * to disable account in the target system
	 * Example : to disable account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to disable account in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean disableAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {

		return null;
	}
	/**
	 * to unlock account in the target system
	 * Example : to disable account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to disable account in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean unLockAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}
	/**
	 * to enable account in the target system
	 * Example : to enable account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to enable account in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean enableAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}
	/**
	 * to terminate account in the target system
	 * Example : to terminate account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to terminate account in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the integer number of accounts terminated
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Integer terminateAccount(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}
	/**
	 * to remove account in the target system
	 * Example : to remove account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to remove account in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the integer number of accounts removed
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Integer removeAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {

		logger.info("Enter DatabaseConnectorService removeAccount");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "RemoveAccountJSON");
			logger.info("End DatabaseConnectorService removeAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount;

	}
	/**
	 * to add access to account in the target system
	 * Example : to add access to account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to add access to account in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the integer access granted count
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Integer addAccessToAccount(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.info("Enter DatabaseConnectorService AddAccessToAccountJSON");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "AddAccessToAccountJSON");
			logger.info("End DatabaseConnectorService AddAccessToAccountJSON");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount;
	}
	/**
	 * to remove access to account in the target system
	 * Example : to remove access to account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to remove access to account in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the integer
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Integer removeAccessToAccount(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.info("Enter DatabaseConnectorService RemoveAccessToAccountJSON");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "RemoveAccessToAccountJSON");
			logger.info("End DatabaseConnectorService RemoveAccessToAccountJSON");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount;
	}
	/**
	 * to change password in the target system
	 * Example : to change password in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to change password in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean changePassword(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}
	/**
     * to create user in the target system
	 * Example : to create user in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to create user in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
     */
	@Override
	public Boolean createUser(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService createUser");
		Integer resultCount = 0;

		try {

			resultCount = executeInputQuery(configData, data, "CreateUserJSON");
			logger.info("End DatabaseConnectorService createUser");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount > 0 ? true : false;
	}
	 /**
		 * to update user in the target system
		 * Example : to update user in the target system , refer to the below steps
		 * step 1 : retrieve connection attributes from configData/Data
		 * step 2 : connect to the target system
		 * step 3 : execute the query/process the required input to update user in the target system
		 * step 4 : return the number of records updated
		 *
		 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
		 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
		 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
		 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
		 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
		 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
		 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
		 *                   for target connection information and other system configuration attributes. configData holds all these
		 *                   inputed details of target connection and system configuration.
		 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
		 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
		 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
		 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
		 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
		 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
		 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
		 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
		 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
		 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
		 * @return the integer
		 * @throws ConnectorException the connector exception
		 */
	@Override
	public Integer updateUser(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService updateUser");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "UpdateUserJSON");
			logger.info("End DatabaseConnectorService updateUser");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount;
	}
	/**
	 * to update the entitlement in target system
	 * Example : to update entitlement in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to update entitlement in the target system
	 * step 4 : return the number of records updated
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Integer updateEntitlement(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.info("Enter DatabaseConnectorService updateEntitlement");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "UpdateEntitlementJSON");
			logger.info("End DatabaseConnectorService updateEntitlement");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount;
	}
	/**
	 * to create the entitlement in target system
	 * Example : to create entitlement in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to create entitlement in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean createEntitlement(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.info("Enter DatabaseConnectorService createEntitlement");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "CreateEntitlementJSON");
			logger.info("End DatabaseConnectorService createEntitlement");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount > 0 ? true : false;
	}
	/**
	 * to validate credentials of the given input from connection
	 * Example : to validate credentials in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process the required input to validate credentials in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean validateCredentials(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}
	/**
	 * to get the summary of number of records for the given input object such as account
	 * Example : to get the summary of number of records for the given input object such as account, refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : set the data with filters if any
	 * step 3 : call getObjectList
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 * @param data       In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name:
	 *                   sampleconntype) connection attributes are defined for  different tasks such as reconciliation,
	 *                   provisioning(createaccount,addaccesstoaccount etc) in the form of JSON's(Example:ReconcileJSON,addAccesstToAccountJSON) for the selected sampleconnector.Upon having
	 *                   a connection Type for sampleconnector, a new connection is to be created.At the time of creating a new connection,
	 *                   ConnectionType(Example:sampleconntype) is chosen and connection attributes are dynamically populated. These
	 *                   connection attributes need to be inputed with relative data.For Example, ReconcileJSON need to be inputed with
	 *                   objects Users,Account,Entitlement (Example: "ACCOUNT" : [ { "saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 * 					"ACCOUNT_ATTRIBUTES" : [ {"saviyntproperty": "Inputsaviyntproperty", "sourceproperty": "${Inputsourceproperty}" } ],
	 *                   "USERS" : [ "saviyntproperty": "Inputsaviyntproperty","sourceproperty": "${Inputsourceproperty}" } ], "ENTITLEMENT" : [ {"saviyntproperty": "Inputsaviyntproperty",
	 *                   "sourceproperty": "${Inputsourceproperty}" } ] data holds all these inputed details of JSON attributes from connection
	 * @return the summary map with object and count as key ,value 
	 */
	@Override
	public Map<String, Object> getSummary(Map<String, Object> configData, Map<String, Object> data) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("Account", 10);
		return map;
	}
	/**
	 * to set the config with attributes needed for creating a connection in SSM.
	 * The attributes defined in setConfig are the attributes that would dynamically
	 * populate on connection creation under SSM
	 *
	 * @param configData In Saviynt Security Manager(SSM/ECM),when creating a new ConnectionType(Example: ConnectionType name: sampleconntype) connection attributes are defined for the
	 *                   target connection information such as system url,username,password etc and other system configuration  attributes such as ECM_INSTANCE_URL
	 *                   ,ECM_INSTANCE_SERVICE_ACCOUNT_NAME,ECM_INSTANCE_SERVICE_ACCOUNT_PASSWORD,STATUSKEYJSON, STATUS_THRESHOLD_CONFIG_JSON(Example :'statusColumn':'customproperty30','activeStatus':
	 *                   ['512','active'],'deleteLinks':true,'accountThresholdValue':1000,'correlateInactiveAccounts':false,'inactivateAccountsNotInFile':false}
	 *                   ) for the selected sampleconnector.Upon having a connection Type for sampleconnector, a new connection is to be
	 *                   created.At the time of creating a new connection, ConnectionType(Example:sampleconntype) is chosen and
	 *                   connection attributes are dynamically populated. These connection attributes need to be inputed with relative data
	 *                   for target connection information and other system configuration attributes. configData holds all these
	 *                   inputed details of target connection and system configuration.
	 */
	@Override
	public void setConfig() {

		List<String> connectionAttributes = configData.getConnectionAttributes();
		connectionAttributes.add("drivername");
		connectionAttributes.add("username");

		connectionAttributes.add("password");
		connectionAttributes.add("url");
		connectionAttributes.add("ReconsileJSON");
		connectionAttributes.add("RemoveAccountJSON");
		connectionAttributes.add("RemoveAccessToAccountJSON");
		connectionAttributes.add("AddAccessToAccountJSON");
		connectionAttributes.add("UpdateAccountJSON");
		connectionAttributes.add("CreateAccountJSON");
		connectionAttributes.add("LockAccountJSON");
		connectionAttributes.add("CreateUserJSON");
		connectionAttributes.add("UpdateUserJSON");
		connectionAttributes.add("UpdateEntitlementJSON");
		connectionAttributes.add("CreateEntitlementJSON");
		connectionAttributes.add("ChangePasswordJSON");
		connectionAttributes.add("UserReconJSON");
		connectionAttributes.add("AccountIncrementalReconField");

		List<String> requiredConnectionAttributes = configData.getRequiredConnectionAttributes();
		requiredConnectionAttributes.add("drivername");
		requiredConnectionAttributes.add("username");
		requiredConnectionAttributes.add("password");
		requiredConnectionAttributes.add("url");

		List<String> encryptedConnectionAttributes = configData.getEncryptedConnectionAttributes();
		encryptedConnectionAttributes.add("password");

		String ConnectionAttributesDescription = configData.getConnectionAttributesDescription();

		JSONObject jsonObject = new JSONObject(ConnectionAttributesDescription);

		for (String k : connectionAttributes) {
			if (k.endsWith("JSON")) {

			} else {
				jsonObject.put(k, "Value of  " + k);
			}
		}
		jsonObject.put("ReconsileJSON",
				"SAMPLE JSON For    {'updatedUser':'SaviyntAdmin','reconType':'fullrecon','query':'  select * from accountrecon ','endpointId':61,'formatterClass':'com.saviynt.ssm.abstractConnector.AbstractFormatter','mapper':{'accounts':[{'saviyntproperty':'name','sourceproperty':'${SamAccountNAme}'}],'account_attributes':[{'saviyntproperty':'attribute_name','sourceproperty':'${Emailaddress}'}],'account_entitlements':[{'saviyntproperty':'entitlementtype','sourceproperty':'${ADgroup}'},{'saviyntproperty':'entitlement_value','sourceproperty':'${AdDristributionlist}'},{'saviyntproperty':'name','sourceproperty':'${SamAccountNAme}'}]}}");
		jsonObject.put("RemoveAccessToAccountJSON", "SAMPLE JSON For    {'query':['Valid Sql Query']} ");
		jsonObject.put("RemoveAccountJSON", "SAMPLE JSON For    {'query':['Valid Sql Query']}");
		jsonObject.put("AddAccessToAccountJSON", "SAMPLE JSON  {'query':['Valid Sql Query']}");
		jsonObject.put("CreateAccountJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("CreateUserJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("UpdateUserJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("UpdateEntitlementJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("CreateEntitlementJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("ChangePasswordJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");

		jsonObject.put("UserReconJSON",
				"SAMPLE JSON For {'importsettings':{'zeroDayProvisioning':true,'userNotInFileAction':'NOACTION','checkRules':true,'buildUserMap':false,'generateSystemUsername':false,'generateEmail':false},'query':' SELECT  * from recontest  ','mapper':{'description':'This is the mapping field for Saviynt Field name','defaultrole':'ROLE_ADMIN','dateformat':'','mapfield':[{'saviyntproperty':'username','sourceproperty':'${username}'},{'saviyntproperty':'firstname','sourceproperty':'${firstname}'}]},'formatterClass':'com.saviynt.ssm.abstractConnector.AbstractFormatter'}");

		configData.setConnectionAttributesDescription(jsonObject.toString());

	}

	/**
	 * Read database properties for batch size
	 * 
	 * @return
	 */
	private Integer batchSize() {

		int batchSize= 0;
		batchSize = Integer.valueOf(loadProperties().getProperty("jdbc.batch.size"));
			if (batchSize <= 0) {

				batchSize = Integer.MIN_VALUE;
			}
			return batchSize;
		} 
	
	/**
	 * To load database config properties
	 * @return
	 */
	private Properties loadProperties() {
		Properties prop = new Properties();
		try (InputStream input = getClass().getClassLoader().getResourceAsStream("databaseconfig.properties")) {

			if (input == null) {
				logger.error("unable to find loadProperties" + input);
			}
			prop.load(input);
			
	}catch(Exception e) {
		logger.error("Error loading in property exception");
		
	}		
		return prop;

	}
	/**
	 * To execute input queries in the target system
	 * @param configData
	 * @param data
	 * @param key
	 * @return
	 */
	private static Integer executeInputQuery(Map<String, Object> configData, Map<String, Object> data, String key) {

		Connection con = null;
		Integer resultCount = 0;

		try {
			con = getConnection(data);
			if (data.containsKey(key)) {
				JSONObject jsonObject = new JSONObject(data.get(key).toString());
				JSONArray query = jsonObject.getJSONArray("query");
				Statement stmt = con.createStatement();
				for (int i = 0; i < query.length(); i++) {
					String queryStr = GroovyService.convertTemplateToString(query.getString(i), data);
					stmt.addBatch(queryStr);
				}
				resultCount = stmt.executeBatch().length;

			}

			return resultCount;
		} catch (Exception e) {
			throw new ConnectorException(e);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					throw new ConnectorException(e);
				}
			}

		}
	}
	/**
	 * to Map input properties to Saviynt properties
	 * @param data
	 * @param con
	 * @param lastRunDate
	 * @param incrField
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<List<Map<String, Object>>> mapPropertiesToSaviynt(Map<String, Object> data, Connection con,
			String lastRunDate,String incrField) {


		String queryInput = data.get("query").toString();
		String query = "";
		if (lastRunDate != null) {
			query = queryInput.concat(" where DATE_FORMAT("+incrField+",'yyyy-MM-dd HH:mm:ss') >= '" +lastRunDate+"'");
		} else {
			query = queryInput;
		}
		List<List<Map<String, Object>>> accountsAllDataMAp = new ArrayList<List<Map<String, Object>>>();
		try {

			Map<String, Object> mapperMap = (Map<String, Object>) data.get("mapper");
			accountsAllDataMAp = getDataFromTableAndMatchProperties(con, mapperMap, query);

		} catch (Exception e) {

			logger.error("Error" + e.getMessage());
		}
		return accountsAllDataMAp;

	}
	/**
	 * retrieve input properties and match with saviynt and return the resultset
	 * @param con
	 * @param mapperMap
	 * @param query
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private List<List<Map<String, Object>>> getDataFromTableAndMatchProperties(Connection con,
			Map<String, Object> mapperMap, String query) throws Exception {

		Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(batchSize());
		ResultSet rs = stmt.executeQuery(query);
		ResultSetMetaData rsmd = rs.getMetaData();

		List<List<Map<String, Object>>> allAccountsFinalList = new ArrayList<List<Map<String, Object>>>();

		while (rs.next()) {

			Map<String, Object> resultsetMap = new HashMap<String, Object>();
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {

				String column = rsmd.getColumnLabel(i);
				resultsetMap.putIfAbsent(column, rs.getString(i));
			}
			List<Map<String, Object>> tempList = new ArrayList<Map<String, Object>>();
			for (String maperKey : mapperMap.keySet()) {

				Map<String, Object> oneRow = new HashMap<String, Object>();

				List<Map<String, String>> valMap = (List<Map<String, String>>) mapperMap.get(maperKey);
				for (Map<String, String> columnPropertyMap : valMap) {
					oneRow.put(maperKey.toUpperCase() + "." + columnPropertyMap.get("saviyntproperty").toUpperCase(),
							GroovyService.convertTemplateToString(columnPropertyMap.get("sourceproperty"),
									resultsetMap));

				}
				tempList.add(oneRow);

			}
			allAccountsFinalList.add(tempList);
		}

		return allAccountsFinalList;
	}
	/**
	 * set the input data from SSM for reconciliation 
	 * @param con
	 * @param mapperMap
	 * @param query
	 * @return
	 * @throws Exception
	 */
	@Override
	public Map<String, List<Map<String, Object>>> accountReconcile(Map<String, Object> configData,
			Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService accountReconcile" + data);

		Map<String, List<Map<String, Object>>> accountMap = new HashMap<String, List<Map<String, Object>>>();
		try {
			logger.debug("data" + data);
			accountMap = processAccountReconcile(configData, data,
					Long.valueOf(data.get("endpointId").toString()));

			logger.info("End DatabaseConnectorService accountReconcile");

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ConnectorException(e);
		}
		return accountMap;
	}
	/**
	 * process the input data from SSM for reconciliation 
	 * @param con
	 * @param mapperMap
	 * @param query
	 * @return
	 * @throws Exception
	 */
	private Map<String, List<Map<String, Object>>> processAccountReconcile(Map<String, Object> configData1,
			Map<String, Object> dataEcm, Long endPointId) {
	 
		JSONObject jsonObject = new JSONObject(dataEcm.get("ReconsileJSON").toString());
		Map<String, Object> data = jsonObject.toMap();
		Connection con = null;
		Map<String, List<Map<String, Object>>> dataMap = new HashMap<String, List<Map<String, Object>>>();
		String lastRunDate = null;
		String incrField = null;
		try {
			con = getConnection(dataEcm);
			
			if(dataEcm.containsKey("IncrementalRecon") && dataEcm.containsKey("AccountIncrementalReconField")) {
				
			  incrField =  dataEcm.get("AccountIncrementalReconField").toString();
				if(incrField==null || incrField.isEmpty()) {
					
					incrField = loadProperties().getProperty("default.account.incremental.field");
					
				}
				
				lastRunDate = getLastRunDate(incrField);
			}
			 
			List<List<Map<String, Object>>> temp = mapPropertiesToSaviynt(data, con,lastRunDate,incrField );

			RepositoryReconService.notify(temp, endPointId, null, dataEcm);

			return dataMap;

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ConnectorException(e);

		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					throw new ConnectorException(e);
				}
			}

		}
	}
	/**
	 * identify the last run date of incremental reconciliation from Saviynt
	 * @param incrField
	 * @return
	 * @throws Exception
	 */
	private String getLastRunDate(String incrField) throws Exception {
	    String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";


		DateFormat df = new SimpleDateFormat(DATE_FORMAT);
        DateTimeFormatter format = DateTimeFormatter.ofPattern(DATE_FORMAT);


		SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
		String dateStr = df.format(RepositoryReconService.getLastRunDate(incrField));
		
		 ZonedDateTime zdt = ZonedDateTime.ofInstant(formatter.parse(dateStr).toInstant(), ZoneId.of("UTC"));
		 //String dtm = format.format(zdt);

		return format.format(zdt);//formatter.parse(dtm);

	}

}
