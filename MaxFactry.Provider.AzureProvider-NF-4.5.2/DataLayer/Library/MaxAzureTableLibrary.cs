// <copyright file="MaxAzureTableLibrary.cs" company="Lakstins Family, LLC">
// Copyright (c) Brian A. Lakstins (http://www.lakstins.com/brian/)
// </copyright>

#region License
// <license>
// This software is provided 'as-is', without any express or implied warranty. In no
// event will the author be held liable for any damages arising from the use of this
// software.
//
// Permission is granted to anyone to use this software for any purpose, including
// commercial applications, and to alter it and redistribute it freely, subject to the
// following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not claim that
// you wrote the original software. If you use this software in a product, an
// acknowledgment (see the following) in the product documentation is required.
//
// Portions Copyright (c) Brian A. Lakstins (http://www.lakstins.com/brian/)
//
// 2. Altered source versions must be plainly marked as such, and must not be
// misrepresented as being the original software.
//
// 3. This notice may not be removed or altered from any source distribution.
// </license>
#endregion License

#region Change Log
// <changelog>
// <change date="6/17/2014" author="Brian A. Lakstins" description="Initial Release">
// <change date="6/23/2014" author="Brian A. Lakstins" description="Update filter for PartitionKey and RowKey.">
// <change date="6/24/2014" author="Brian A. Lakstins" description="Add handling of long type.">
// <change date="6/25/2014" author="Brian A. Lakstins" description="Add handling of double type.">
// <change date="6/26/2014" author="Brian A. Lakstins" description="Update for addition of StorageKey.">
// <change date="7/2/2014" author="Brian A. Lakstins" description="Add support for storing byte[].">
// <change date="7/3/2014" author="Brian A. Lakstins" description="Updated to filter data fields that are stored.">
// <change date="8/13/2014" author="Brian A. Lakstins" description="Added logging.">
// <change date="8/21/2014" author="Brian A. Lakstins" description="Moved from AzureTable to AzureStorage.">
// <change date="10/22/2014" author="Brian A. Lakstins" description="Fix to make sure Guid stored as string in RowKey and PartitionKey are stored in lower case.">
// <change date="11/10/2014" author="Brian A. Lakstins" description="Updates for changes to core.">
// <change date="1/14/2015" author="Brian A. Lakstins" description="Updates to not allow null in bool types.">
// <change date="6/7/2015" author="Brian A. Lakstins" description="Add more log details.">
// <change date="1/3/2016" author="Brian A. Lakstins" description="Fix decompressing long text.">
// <change date="1/3/2016" author="Brian A. Lakstins" description="Only insert or update changed properties.  Store long text like a stream.">
// <change date="1/10/2016" author="Brian A. Lakstins" description="Store raw byte arrays.">
// <change date="5/18/2016" author="Brian A. Lakstins" description="Speed up table exists checks by getting list of all tables when first table is checked.">
// <change date="7/12/2016" author="Brian A. Lakstins" description="Add ability to specify name of file to use in url for stored stream.">
// <change date="9/2/2016" author="Brian A. Lakstins" description="Udpate to insert failure process.">
// <change date="12/21/2016" author="Brian A. Lakstins" description="Update to allow lazy loading of stream data.">
// <change date="12/21/2016" author="Brian A. Lakstins" description="Update to use PrimaryKey Suffix for PartitionKey.">
// <change date="12/21/2016" author="Brian A. Lakstins" description="Update to start storing StorageKey in every row.">
// <change date="2/16/2018" author="Brian A. Lakstins" description="Renamed extended properties so they don't get stored.">
// <change date="5/1/2018" author="Brian A. Lakstins" description="Added RowKey to filtering by Id to use Index and make it faster.">
// <change date="11/30/2018" author="Brian A. Lakstins" description="Updated for changes to base.">
// <change date="10/23/2019" author="Brian A. Lakstins" description="Updates to allow some blobs to be private and some to be public.">
// <change date="12/11/2019" author="Brian A. Lakstins" description="Tweaks to query processes.  Allow selecting  records without using Partition Key as part of key for query.">
// <change date="3/31/2024" author="Brian A. Lakstins" description="Updated for changes to dependency classes.">
// <change date="5/13/2025" author="Brian A. Lakstins" description="Updated for new Id based class.">
// <change date="5/29/2025" author="Brian A. Lakstins" description="Update filtering for specifying PartitionKey and RowKey">
// </changelog>
#endregion Change Log

namespace MaxFactry.Provider.AzureProvider.DataLayer
{
    using System;
    using MaxFactry.Core;
    using MaxFactry.Base.DataLayer;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Provides session services using MaxFactryLibrary.
    /// </summary>
    public class MaxAzureTableLibrary
    {
        /// <summary>
        /// List of tables that have been created.
        /// </summary>
        private static MaxIndex _oTableList = new MaxIndex();

        /// <summary>
        /// Object to lock access to _oTableList
        /// </summary>
        private static object _oLock = new object();

        /// <summary>
        /// Minimum value of a date and time stored in Azure table.
        /// </summary>
        private static DateTime _dAzureDateTimeMin = new DateTime(1601, 1, 1);

        /// <summary>
        /// Text in string field to indicate that the actual value is in a stream.
        /// </summary>
        private static string _sAzureStringStreamIndicator = "StringStream" + new Guid("{84FCAB1B-34AA-4C65-AD66-6CF3BF3873EC}").ToString();

        /// <summary>
        /// Text in string field to indicate that the value is compressed
        /// </summary>
        private static string _sAzureStringCompressedIndicator = "StringCompresed" + new Guid("{84FCAB1B-34AA-4C65-AD66-6CF3BF3873EC}").ToString();

        private static string _sDefaultPartitionKey = null;

        /// <summary>
        /// Gets the Minimum value of a date and time stored in Azure table.
        /// </summary>
        public static DateTime AzureDateTimeMin
        {
            get { return _dAzureDateTimeMin; }
        }

        /// <summary>
        /// Gets the Text in string field to indicate that the actual value is in a stream.
        /// </summary>
        public static string AzureStringStreamIndicator
        {
            get { return _sAzureStringStreamIndicator; }
        }

        /// <summary>
        /// Gets the Text in string field to indicate that the value is compressed
        /// </summary>
        public static string AzureStringCompressedIndicator
        {
            get { return _sAzureStringCompressedIndicator; }
        }

        public static string DefaultPartitionKey
        {
            get
            {
                if (null == _sDefaultPartitionKey)
                {
                    lock (_oLock)
                    {
                        if (null == _sDefaultPartitionKey)
                        {
                            _sDefaultPartitionKey = MaxConvertLibrary.ConvertToString(typeof(object), MaxConfigurationLibrary.GetValue(MaxEnumGroup.ScopeApplication, MaxFactryLibrary.MaxStorageKeyName)).ToLower();
                            if (null == _sDefaultPartitionKey)
                            {
                                _sDefaultPartitionKey = "SinglePartition";
                            }
                        }
                    }
                }

                return _sDefaultPartitionKey;
            }
        }

        /// <summary>
        /// Selects all data from the table
        /// </summary>
        /// <param name="lsAccountName">Azure Storage Account Name</param>
        /// <param name="lsAccountKey">Azure Storage Account Key</param>
        /// <param name="loData">The table name</param>
        /// <returns>List of MaxData with all data</returns>
        public static MaxDataList SelectAll(string lsAccountName, string lsAccountKey, MaxData loData)
        {
            string lsTable = loData.DataModel.DataStorageName;
            CloudTableClient loTableClient = GetTableClient(lsAccountName, lsAccountKey, lsTable);
            CloudTable loTable = loTableClient.GetTableReference(lsTable);
            TableQuery loQuery = new TableQuery();
            MaxDataList loDataList = new MaxDataList(loData.DataModel);
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableQueryFilter", MaxFactry.Core.MaxEnumGroup.LogDebug, "select from {TableName}", loTable.Name));
            foreach (DynamicTableEntity loDataEntity in loTable.ExecuteQuery(loQuery))
            {
                MaxData loDataOut = new MaxData(loData.DataModel);
                foreach (string lsKey in loDataEntity.Properties.Keys)
                {
                    if (loDataEntity.Properties[lsKey].PropertyType == EdmType.Boolean)
                    {
                        loDataOut.Set(lsKey, loDataEntity.Properties[lsKey].BooleanValue);
                    }
                    else if (loDataEntity.Properties[lsKey].PropertyType == EdmType.DateTime)
                    {
                        DateTime ldValue = loDataEntity.Properties[lsKey].DateTimeOffsetValue.Value.UtcDateTime;
                        if (ldValue.Equals(AzureDateTimeMin))
                        {
                            loDataOut.Set(lsKey, DateTime.MinValue);
                        }
                        else
                        {
                            loDataOut.Set(lsKey, ldValue);
                        }
                    }
                    else if (loDataEntity.Properties[lsKey].PropertyType == EdmType.Double)
                    {
                        loDataOut.Set(lsKey, loDataEntity.Properties[lsKey].DoubleValue);
                    }
                    else if (loDataEntity.Properties[lsKey].PropertyType == EdmType.Guid)
                    {
                        loDataOut.Set(lsKey, loDataEntity.Properties[lsKey].GuidValue);
                    }
                    else if (loDataEntity.Properties[lsKey].PropertyType == EdmType.Int32)
                    {
                        loDataOut.Set(lsKey, loDataEntity.Properties[lsKey].Int32Value);
                    }
                    else if (loDataEntity.Properties[lsKey].PropertyType == EdmType.Int64)
                    {
                        loDataOut.Set(lsKey, loDataEntity.Properties[lsKey].Int64Value);
                    }
                    else if (loDataEntity.Properties[lsKey].PropertyType == EdmType.String)
                    {
                        string lsValue = loDataEntity.Properties[lsKey].StringValue;
                        if (lsValue.StartsWith(AzureStringCompressedIndicator))
                        {
                            lsValue = lsValue.Substring(0, AzureStringCompressedIndicator.Length);
                            byte[] laValue = MaxCompressionLibrary.Decompress(
                                typeof(string),
                                Convert.FromBase64String(lsValue));
                            lsValue = System.Text.UTF8Encoding.UTF8.GetString(laValue);
                        }
                    }
                }

                loDataOut.Set("_Timestamp", loDataEntity.Timestamp);
                loDataOut.Set("_RowKey", loDataEntity.RowKey);
                loDataOut.Set("_PartitionKey", loDataEntity.PartitionKey);
                MaxBaseDataModel loBaseDataModel = loData.DataModel as MaxBaseDataModel;
                if (null != loBaseDataModel && loBaseDataModel.IsStored(loBaseDataModel.StorageKey))
                {
                    loDataOut.Set(loBaseDataModel.StorageKey, loDataEntity.PartitionKey);
                }

                loDataOut.ClearChanged();
                loDataList.Add(loDataOut);
            }

            return loDataList;
        }

        /// <summary>
        /// Selects data from the database.
        /// </summary>
        /// <param name="lsAccountName">Azure Account Name</param>
        /// <param name="lsAccountKey">Azure Account Key</param>
        /// <param name="loQuery">Azure Table Storage Query</param>
        /// <param name="loData">Element with data used in the filter.</param>
        /// <param name="lnPageIndex">Page to return.</param>
        /// <param name="lnPageSize">Items per page.</param>
        /// <param name="lnTotal">Total items found.</param>
        /// <returns>List of data from select.</returns>
        public static MaxDataList Select(string lsAccountName, string lsAccountKey, string lsContainer, TableQuery loQuery, MaxData loData, int lnPageIndex, int lnPageSize, out int lnTotal)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            CloudTableClient loTableClient = MaxAzureTableLibrary.GetTableClient(lsAccountName, lsAccountKey, loData.DataModel.DataStorageName);
            CloudTable loTable = loTableClient.GetTableReference(loData.DataModel.DataStorageName);

            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDataModel", MaxFactry.Core.MaxEnumGroup.LogDebug, "{DataModelType} after getting cloud table reference", loData.DataModel.GetType()));
            MaxDataList loDataList = new MaxDataList(loData.DataModel);
            int lnRows = 0;
            int lnStart = 0;
            int lnEnd = int.MaxValue;
            if (lnPageSize > 0 && lnPageSize < int.MaxValue)
            {
                if (lnPageIndex > 0)
                {
                    lnStart = lnPageIndex * lnPageSize;
                }

                lnEnd = lnStart + lnPageSize;
            }

            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableQueryFilter", MaxFactry.Core.MaxEnumGroup.LogDebug, "select from {TableName} where {FilterString}", loTable.Name, loQuery.FilterString));
            foreach (DynamicTableEntity loDataEntity in loTable.ExecuteQuery(loQuery))
            {
                if (lnRows >= lnStart && lnRows < lnEnd)
                {
                    MaxData loDataOut = new MaxData(loData);
                    foreach (string lsDataName in loData.DataModel.DataNameList)
                    {
                        if (loDataEntity.Properties.ContainsKey(lsDataName))
                        {
                            EntityProperty loProperty = loDataEntity.Properties[lsDataName];
                            if (null != loProperty)
                            {
                                Type loValueType = loData.DataModel.GetValueType(lsDataName);
                                if (typeof(Guid).Equals(loValueType))
                                {
                                    loDataOut.Set(lsDataName, loProperty.GuidValue);
                                }
                                else if (typeof(bool).Equals(loValueType))
                                {
                                    loDataOut.Set(lsDataName, loProperty.BooleanValue);
                                }
                                else if (typeof(DateTime).Equals(loValueType))
                                {
                                    DateTime ldValue = loProperty.DateTimeOffsetValue.Value.UtcDateTime;
                                    if (ldValue.Equals(MaxAzureTableLibrary.AzureDateTimeMin))
                                    {
                                        loDataOut.Set(lsDataName, DateTime.MinValue);
                                    }
                                    else
                                    {
                                        loDataOut.Set(lsDataName, ldValue);
                                    }
                                }
                                else if (typeof(double).Equals(loValueType))
                                {
                                    loDataOut.Set(lsDataName, loProperty.DoubleValue);
                                }
                                else if (typeof(int).Equals(loValueType))
                                {
                                    loDataOut.Set(lsDataName, loProperty.Int32Value);
                                }
                                else if (typeof(long).Equals(loValueType))
                                {
                                    loDataOut.Set(lsDataName, loProperty.Int64Value);
                                }
                                else if (typeof(string).Equals(loValueType) ||
                                    typeof(MaxShortString).Equals(loValueType) ||
                                    typeof(MaxLongString).Equals(loValueType))
                                {
                                    string lsValue = loProperty.StringValue;
                                    if (lsValue.Equals(AzureStringStreamIndicator, StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        lsValue = MaxDataModel.StreamStringIndicator;
                                    }
                                    else if (lsValue.StartsWith(AzureStringCompressedIndicator))
                                    {
                                        lsValue = lsValue.Substring(AzureStringCompressedIndicator.Length);
                                        byte[] laValue = MaxCompressionLibrary.Decompress(
                                            typeof(string),
                                            Convert.FromBase64String(lsValue));
                                        lsValue = System.Text.UTF8Encoding.UTF8.GetString(laValue);
                                    }

                                    loDataOut.Set(lsDataName, lsValue);
                                }
                                else if (typeof(byte[]).Equals(loValueType))
                                {
                                    loDataOut.Set(lsDataName, loProperty.PropertyAsObject as byte[]);
                                }
                            }
                        }
                    }

                    loDataOut.Set("_Timestamp", loDataEntity.Timestamp);
                    loDataOut.Set("_RowKey", loDataEntity.RowKey);
                    loDataOut.Set("_PartitionKey", loDataEntity.PartitionKey);
                    loDataOut.ClearChanged();
                    loDataList.Add(loDataOut);
                }

                lnRows++;
            }

            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableQueryComplete", MaxFactry.Core.MaxEnumGroup.LogWarning, "select from {TableName} where {FilterString} got {RowCount} in {Milliseconds}", loTable.Name, loQuery.FilterString, lnRows, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableQueryComplete", MaxFactry.Core.MaxEnumGroup.LogDebug, "select from {TableName} where {FilterString} got {RowCount} in {Milliseconds}", loTable.Name, loQuery.FilterString, lnRows, loWatch.Elapsed.TotalMilliseconds));
            }

            lnTotal = lnRows;
            return loDataList;
        }

        /// <summary>
        /// Inserts a new data element.
        /// </summary>
        /// <param name="lsAccountName">Azure Storage Account Name</param>
        /// <param name="lsAccountKey">Azure Storage Account Key</param>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>The data that was inserted.</returns>
        public static int Insert(string lsAccountName, string lsAccountKey, MaxDataList loDataList)
        {
            int lnR = 0;
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsertStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {DataStorageName} for {DataCount}.", loDataList.DataModel.DataStorageName, loDataList.Count));
            for (int lnD = 0; lnD < loDataList.Count; lnD++)
            {
                System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
                MaxData loData = loDataList[lnD];
                CloudTableClient loTableClient = GetTableClient(lsAccountName, lsAccountKey, loData.DataModel.DataStorageName);

                CloudTable loTable = loTableClient.GetTableReference(loData.DataModel.DataStorageName);
                DynamicTableEntity loDynamicTableEntity = GetDynamicTableEntityForSingleMatch(loData);
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsertStartOne", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {TableName}", loTable.Name));
                TableOperation loOperation = TableOperation.Insert(loDynamicTableEntity);
                TableResult loResult = null;
                try
                {
                    loResult = loTable.Execute(loOperation);
                }
                catch (Exception loE)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsert", MaxFactry.Core.MaxEnumGroup.LogError, "insert {TableName} failed.", loE, loTable.Name));
                }

                if (null != loResult)
                {
                    if (200 <= loResult.HttpStatusCode && loResult.HttpStatusCode < 300)
                    {
                        lnR++;
                        loDataList[lnD].ClearChanged();
                    }
                    else
                    {
                        MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsert", MaxFactry.Core.MaxEnumGroup.LogError, "insert {TableName} failed with code {StatusCode}.", loTable.Name, loResult.HttpStatusCode));
                    }
                }

                loWatch.Stop();
                if (loWatch.Elapsed.TotalMilliseconds > 100)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsertEndOne", MaxFactry.Core.MaxEnumGroup.LogWarning, "insert {TableName} in {Milliseconds}", loTable.Name, loWatch.Elapsed.TotalMilliseconds));
                }
                else
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsertEndOne", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {TableName} in {Milliseconds}", loTable.Name, loWatch.Elapsed.TotalMilliseconds));
                }
            }

            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsertEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {DataStorageName} for {DataCount} inserted {RowCount} rows.", loDataList.DataModel.DataStorageName, loDataList.Count, lnR));
            return lnR;
        }

        /// <summary>
        /// Updates an existing data element.
        /// </summary>
        /// <param name="lsAccountName">Azure Storage Account Name</param>
        /// <param name="lsAccountKey">Azure Storage Account Key</param>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>The data that was updated.</returns>
        public static int Update(string lsAccountName, string lsAccountKey, MaxDataList loDataList)
        {
            int lnR = 0;
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {DataStorageName} for {DataCount}.", loDataList.DataModel.DataStorageName, loDataList.Count));
            for (int lnD = 0; lnD < loDataList.Count; lnD++)
            {
                System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
                MaxData loData = loDataList[lnD];
                CloudTableClient loTableClient = GetTableClient(lsAccountName, lsAccountKey, loData.DataModel.DataStorageName);
                CloudTable loTable = loTableClient.GetTableReference(loData.DataModel.DataStorageName);

                DynamicTableEntity loDynamicTableEntity = GetDynamicTableEntityForSingleMatch(loData);
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateStartOne", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {TableName}", loTable.Name));
                TableOperation loOperation = TableOperation.InsertOrMerge(loDynamicTableEntity);
                TableResult loResult = null;
                try
                {
                    loResult = loTable.Execute(loOperation);
                }
                catch (Exception loE)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdate", MaxFactry.Core.MaxEnumGroup.LogError, "update {TableName} failed.", loE, loTable.Name));
                }

                if (null != loResult)
                {
                    if (200 <= loResult.HttpStatusCode && loResult.HttpStatusCode < 300)
                    {
                        lnR++;
                        loDataList[lnD].ClearChanged();
                    }
                    else
                    {
                        MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdate", MaxFactry.Core.MaxEnumGroup.LogError, "update {TableName} failed with code {StatusCode}.", loTable.Name, loResult.HttpStatusCode));
                    }
                }

                loWatch.Stop();
                if (loWatch.Elapsed.TotalMilliseconds > 100)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateEndOne", MaxFactry.Core.MaxEnumGroup.LogWarning, "update {TableName} in {Milliseconds}", loTable.Name, loWatch.Elapsed.TotalMilliseconds));
                }
                else
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateEndOne", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {TableName} in {Milliseconds}", loTable.Name, loWatch.Elapsed.TotalMilliseconds));
                }
            }

            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {DataStorageName} for {DataCount} updated {RowCount} rows.", loDataList.DataModel.DataStorageName, loDataList.Count, lnR));
            return lnR;
        }

        /// <summary>
        /// Deletes an existing data element.
        /// </summary>
        /// <param name="lsAccountName">Azure Storage Account Name</param>
        /// <param name="lsAccountKey">Azure Storage Account Key</param>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>true if deleted.</returns>
        public static int Delete(string lsAccountName, string lsAccountKey, MaxDataList loDataList)
        {
            int lnR = 0;
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDeleteStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {DataStorageName} for {DataCount}.", loDataList.DataModel.DataStorageName, loDataList.Count));
            for (int lnD = 0; lnD < loDataList.Count; lnD++)
            {
                System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
                MaxData loData = loDataList[lnD];
                CloudTableClient loTableClient = GetTableClient(lsAccountName, lsAccountKey, loData.DataModel.DataStorageName);
                CloudTable loTable = loTableClient.GetTableReference(loData.DataModel.DataStorageName);
                DynamicTableEntity loDynamicTableEntity = GetDynamicTableEntityForSingleMatch(loData);
                loDynamicTableEntity.ETag = "*";
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateDeleteOne", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {TableName}", loTable.Name));
                TableOperation loOperation = TableOperation.Delete(loDynamicTableEntity);
                TableResult loResult = null;
                try
                {
                    loResult = loTable.Execute(loOperation);
                }
                catch (Exception loE)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDelete", MaxFactry.Core.MaxEnumGroup.LogError, "delete {TableName} failed.", loE, loTable.Name));
                }

                if (null != loResult)
                {
                    if (200 <= loResult.HttpStatusCode && loResult.HttpStatusCode < 300)
                    {
                        lnR++;
                        loDataList[lnD].ClearChanged();
                    }
                    else
                    {
                        MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDelete", MaxFactry.Core.MaxEnumGroup.LogError, "delete {TableName} failed with code {StatusCode}.", loTable.Name, loResult.HttpStatusCode));
                    }
                }

                loWatch.Stop();
                if (loWatch.Elapsed.TotalMilliseconds > 100)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDeleteEndOne", MaxFactry.Core.MaxEnumGroup.LogWarning, "delete {TableName} in {Milliseconds}", loTable.Name, loWatch.Elapsed.TotalMilliseconds));
                }
                else
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDeleteEndOne", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {TableName} in {Milliseconds}", loTable.Name, loWatch.Elapsed.TotalMilliseconds));
                }
            }

            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDeleteEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {DataStorageName} for {DataCount} deleted {RowCount} rows.", loDataList.DataModel.DataStorageName, loDataList.Count, lnR));
            return lnR;
        }

        /// <summary>
        /// Gets a table client for the specified table.  Creates the table if it does not exist.
        /// </summary>
        /// <param name="lsAccountName">Azure Storage Account Name</param>
        /// <param name="lsAccountKey">Azure Storage Account Key</param>
        /// <param name="lsTable">Name of the table.</param>
        /// <returns>Azure Table client.</returns>
        public static CloudTableClient GetTableClient(string lsAccountName, string lsAccountKey, string lsTable)
        {
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableGetClientStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "Start {TableName}", lsTable));
            CloudStorageAccount loAccount = null;
            if (lsAccountName == "UseDevelopmentStorage")
            {
                loAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
            }
            else
            {
                loAccount = new CloudStorageAccount(new StorageCredentials(lsAccountName, lsAccountKey), true);
            }

            CloudTableClient loTableClient = loAccount.CreateCloudTableClient();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableGetClient", MaxFactry.Core.MaxEnumGroup.LogDebug, "Created client {TableName}", lsTable));
            string lsTableKey = lsTable + "|" + lsAccountName;
            if (!_oTableList.Contains(lsTableKey))
            {
                lock (_oLock)
                {
                    if (!_oTableList.Contains(lsTableKey))
                    {
                        foreach (CloudTable loTable in loTableClient.ListTables())
                        {
                            string lsTableKeyExisting = loTable.Name + "|" + lsAccountName;
                            if (!_oTableList.Contains(lsTableKeyExisting))
                            {
                                _oTableList.Add(lsTableKeyExisting, true);
                            }
                        }

                        if (!_oTableList.Contains(lsTableKey))
                        {
                            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableGetClientCreateTableStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "Creating table {TableName}", lsTable));
                            loTableClient.GetTableReference(lsTable).CreateIfNotExists();
                            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableGetClientCreateTableEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "Created table {TableName}", lsTable));
                            _oTableList.Add(lsTableKey, true);
                        }
                    }
                }
            }

            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableGetClientEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "End {TableName}", lsTable));
            return loTableClient;
        }

        /// <summary>
        /// Gets a dynamic table entity based on the data.
        /// </summary>
        /// <param name="loData">The MaxData object containing the data.</param>
        /// <returns>A dynamic table entity.</returns>
        public static DynamicTableEntity GetDynamicTableEntityForSingleMatch(MaxData loData)
        {
            DynamicTableEntity loDynamicTableEntity = new DynamicTableEntity();
            //// Use the partitionkey that came from the query
            string lsPartitionKey = loData.Get("_PartitionKey") as string;
            if (string.IsNullOrEmpty(lsPartitionKey))
            {
                MaxBaseDataModel loBaseDataModel = loData.DataModel as MaxBaseDataModel;
                if (null != loBaseDataModel)
                {
                    lsPartitionKey = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(loBaseDataModel.StorageKey));
                }

                if (string.IsNullOrEmpty(lsPartitionKey))
                {
                    lsPartitionKey = DefaultPartitionKey;
                }

                /*
                string lsPrimaryKeySuffix = loData.DataModel.GetPrimaryKeySuffix(loData);
                if (!string.IsNullOrEmpty(lsPrimaryKeySuffix) && !string.IsNullOrEmpty(lsPartitionKey))
                {
                    if (lsPartitionKey.EndsWith(lsPrimaryKeySuffix))
                    {
                        if (null != loBaseDataModel)
                        {
                            //// The suffix has already been added, so update the storagekey property to remove it.
                            loData.Set(loBaseDataModel.StorageKey, lsPartitionKey.Substring(0, lsPartitionKey.Length - lsPrimaryKeySuffix.Length));
                        }
                    }
                    else if (lsPartitionKey.Length == DefaultPartitionKey.Length)
                    {
                        lsPartitionKey += lsPrimaryKeySuffix;
                    }
                }
                */
            }

            //// The partition key may not be the same as the StorageKey.
            //// It may have more detail so queries can use it as an Index.
            loDynamicTableEntity.PartitionKey = lsPartitionKey;
            foreach (string lsDataName in loData.DataModel.DataNameList)
            {
                if (loData.DataModel.IsStored(lsDataName) || loData.GetIsChanged(lsDataName))
                {
                    object loValueType = loData.DataModel.GetValueType(lsDataName);
                    object loValue = loData.Get(lsDataName);
                    if (null != loValue)
                    {
                        EntityProperty loProperty = null;
                        Type loDataModelType = loData.DataModel.GetType();
                        if (typeof(Guid).Equals(loValueType))
                        {
                            loProperty = new EntityProperty(MaxConvertLibrary.ConvertToGuid(loDataModelType, loValue));
                        }
                        else if (typeof(string).Equals(loValueType) ||
                            typeof(MaxShortString).Equals(loValueType) ||
                            typeof(MaxLongString).Equals(loValueType))
                        {
                            loProperty = new EntityProperty(MaxConvertLibrary.ConvertToString(loDataModelType, loValue));
                        }
                        else if (typeof(bool).Equals(loValueType))
                        {
                            loProperty = new EntityProperty(MaxConvertLibrary.ConvertToBoolean(loDataModelType, loValue));
                        }
                        else if (typeof(DateTime).Equals(loValueType))
                        {
                            DateTime ldValue = MaxConvertLibrary.ConvertToDateTime(loData.DataModel.GetType(), loData.Get(lsDataName));
                            if (ldValue < AzureDateTimeMin)
                            {
                                ldValue = AzureDateTimeMin;
                            }

                            loProperty = new EntityProperty(ldValue);
                        }
                        else if (typeof(int).Equals(loValueType))
                        {
                            loProperty = new EntityProperty(MaxConvertLibrary.ConvertToInt(loDataModelType, loValue));
                        }
                        else if (typeof(long).Equals(loValueType))
                        {
                            loProperty = new EntityProperty(MaxConvertLibrary.ConvertToLong(loDataModelType, loValue));
                        }
                        else if (typeof(double).Equals(loValueType))
                        {
                            loProperty = new EntityProperty(MaxConvertLibrary.ConvertToDouble(loDataModelType, loValue));
                        }
                        else if (typeof(byte[]).Equals(loValueType))
                        {
                            if (loValue is byte[])
                            {
                                loProperty = new EntityProperty((byte[])loValue);
                            }
                        }

                        if (null != loProperty)
                        {
                            loData.ClearChanged(lsDataName);
                            loDynamicTableEntity.Properties.Add(lsDataName, loProperty);
                        }
                    }
                }
            }

            //// Use the rowkey that came from the query
            string lsRowKey = loData.Get("_RowKey") as string;
            if (String.IsNullOrEmpty(lsRowKey))
            {
                if (loData.DataModel is MaxIdGuidDataModel)
                {
                    lsRowKey = loData.Get(((MaxIdGuidDataModel)loData.DataModel).Id).ToString().ToLowerInvariant();
                }
                else if (loData.DataModel is MaxBaseGuidKeyDataModel)
                {
                    lsRowKey = loData.Get(((MaxBaseGuidKeyDataModel)loData.DataModel).Id).ToString().ToLowerInvariant();
                }
                else if (loData.DataModel is MaxIdIntegerDataModel)
                {
                    lsRowKey = loData.Get(((MaxIdIntegerDataModel)loData.DataModel).Id).ToString().ToLowerInvariant();
                }
                else if (loData.DataModel is MaxIdStringDataModel)
                {
                    lsRowKey = loData.Get(((MaxIdStringDataModel)loData.DataModel).Id).ToString().ToLowerInvariant();
                }
                else if (loData.DataModel is MaxBaseRelationDataModel)
                {
                    lsRowKey = loData.Get(((MaxBaseRelationDataModel)loData.DataModel).ParentId).ToString().ToLowerInvariant() +
                        loData.Get(((MaxBaseRelationDataModel)loData.DataModel).ChildId).ToString().ToLowerInvariant();
                    object loRelationType = loData.Get(((MaxBaseRelationDataModel)loData.DataModel).RelationType);
                    if (null != loRelationType)
                    {
                        lsRowKey += loRelationType.ToString().ToLowerInvariant();
                    }
                }
            }

            loDynamicTableEntity.RowKey = lsRowKey;
            return loDynamicTableEntity;
        }

        /// <summary>
        /// Gets the TableQuery to use with the Azure query.
        /// </summary>
        /// <param name="loData">Element with data used in the filter.</param>
        /// <param name="loDataQuery">Query information to filter results.</param>
        /// <param name="laDataNameList">list of fields to return from select.</param>
        /// <returns>TableQuery to use with Azure Tables.</returns>
        public static TableQuery GetTableQueryForSelect(MaxData loData, MaxDataQuery loDataQuery, params string[] laDataNameList)
        {
            string lsPartitionKey = loData.Get("_PartitionKey") as string;
            MaxBaseDataModel loBaseDataModel = loData.DataModel as MaxBaseDataModel;
            if (string.IsNullOrEmpty(lsPartitionKey))
            {
                if (null != loBaseDataModel)
                {
                    lsPartitionKey = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(loBaseDataModel.StorageKey));
                }
                
                if (lsPartitionKey != "*")
                {
                    if (string.IsNullOrEmpty(lsPartitionKey))
                    {
                        lsPartitionKey = DefaultPartitionKey;
                    }

                    /*
                    string lsPrimaryKeySuffix = loData.DataModel.GetPrimaryKeySuffix(loData);
                    if (!string.IsNullOrEmpty(lsPrimaryKeySuffix) && !string.IsNullOrEmpty(lsPartitionKey))
                    {
                        if (lsPartitionKey.EndsWith(lsPrimaryKeySuffix))
                        {
                            if (null != loBaseDataModel)
                            {
                                //// The suffix has already been added, so update the storagekey property to remove it.
                                loData.Set(loBaseDataModel.StorageKey, lsPartitionKey.Substring(0, lsPartitionKey.Length - lsPrimaryKeySuffix.Length));
                            }
                        }
                        else
                        {
                            lsPartitionKey += lsPrimaryKeySuffix;
                        }
                    }
                    */
                }
            }

            TableQuery loQuery = new TableQuery();
            if (null != laDataNameList)
            {
                loQuery.Select(laDataNameList);
            }
            else
            {
                loQuery.Select(loData.DataModel.DataNameList);
            }

            string lsAzureFilter = string.Empty;
            foreach (string lsDataName in loData.DataModel.DataNameList)
            {
                if (loData.DataModel.IsStored(lsDataName))
                {
                    bool lbIsPrimaryKey = loData.DataModel.GetAttributeSetting(lsDataName, "IsPrimaryKey");

                    //// TODO: Add filtering by StorageKey once all data has been updated to save storage key
                    //// 12/21/2016 - StorageKey is not saved in each row at this time.  Filtering is done based on PartitionKey.
                    if (lbIsPrimaryKey && (null != loBaseDataModel && !lsDataName.Equals(loBaseDataModel.StorageKey)))
                    {
                        object loKeyValue = loData.Get(lsDataName);
                        if (null != loKeyValue)
                        {
                            string lsFilterCondition = GetFilterCondition(lsDataName, "=", loKeyValue, loData.DataModel);
                            if (lsAzureFilter.Length > 0)
                            {
                                lsAzureFilter = TableQuery.CombineFilters(lsAzureFilter, TableOperators.And, lsFilterCondition);
                            }
                            else
                            {
                                lsAzureFilter = lsFilterCondition;
                            }

                            if (loData.DataModel is MaxIdGuidDataModel && lsDataName == ((MaxIdGuidDataModel)loData.DataModel).Id)
                            {
                                lsFilterCondition = GetFilterCondition("RowKey", "=", loKeyValue.ToString(), loData.DataModel);
                                lsAzureFilter = TableQuery.CombineFilters(lsAzureFilter, TableOperators.And, lsFilterCondition);
                            }
                        }
                    }
                }
            }

            object[] laDataQuery = loDataQuery.GetQuery();
            if (laDataQuery.Length > 0)
            {
                string lsDataQuery = string.Empty;
                for (int lnDQ = 0; lnDQ < laDataQuery.Length; lnDQ++)
                {
                    object loStatement = laDataQuery[lnDQ];
                    if (loStatement is char)
                    {
                        // Group Start or end characters
                        lsDataQuery += (char)loStatement;
                    }
                    else if (loStatement is string)
                    {
                        // Comparison operators
                        lsDataQuery += " " + ((string)loStatement).ToLower() + " ";
                    }
                    else if (loStatement is MaxDataFilter)
                    {
                        MaxDataFilter loDataFilter = (MaxDataFilter)loStatement;
                        if (loDataFilter.Name.Equals(loBaseDataModel.StorageKey))
                        {
                            if (lsPartitionKey != loDataFilter.Value.ToString() && lsPartitionKey.Length <= loDataFilter.Value.ToString().Length)
                            {
                                string lsPartitionKeyFilter = TableQuery.GenerateFilterCondition(
                                    "PartitionKey",
                                    QueryComparisons.Equal,
                                    loDataFilter.Value.ToString());
                                lsPartitionKey = string.Empty;
                                lsDataQuery += lsPartitionKeyFilter;
                            }
                            else
                            {
                                lsDataQuery += GetFilterCondition(loDataFilter.Name, loDataFilter.Operator, loDataFilter.Value, loData.DataModel);
                            }
                        }
                        else
                        {
                            lsDataQuery += GetFilterCondition(loDataFilter.Name, loDataFilter.Operator, loDataFilter.Value, loData.DataModel);
                            if (loData.DataModel is MaxIdGuidDataModel && loDataFilter.Name == ((MaxIdGuidDataModel)loData.DataModel).Id)
                            {
                                string lsRowKeyFilterCondition = GetFilterCondition("RowKey", "=", loDataFilter.Value.ToString(), loData.DataModel);
                                lsDataQuery = TableQuery.CombineFilters(lsDataQuery, TableOperators.And, lsRowKeyFilterCondition);
                            }
                            else if (loData.DataModel is MaxBaseGuidKeyDataModel && loDataFilter.Name == ((MaxBaseGuidKeyDataModel)loData.DataModel).Id)
                            {
                                string lsRowKeyFilterCondition = GetFilterCondition("RowKey", "=", loDataFilter.Value.ToString(), loData.DataModel);
                                lsDataQuery = TableQuery.CombineFilters(lsDataQuery, TableOperators.And, lsRowKeyFilterCondition);
                            }
                        }
                    }
                }

                if (!string.IsNullOrEmpty(lsAzureFilter))
                {
                    lsAzureFilter = TableQuery.CombineFilters(lsAzureFilter, TableOperators.And, lsDataQuery);
                }
                else
                {
                    lsAzureFilter = lsDataQuery;
                }
            }

            if (lsPartitionKey != "*" && !string.IsNullOrEmpty(lsPartitionKey))
            {
                string lsPartitionKeyFilter = TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    lsPartitionKey);

                if (lsAzureFilter.Length > 0)
                {
                    lsAzureFilter = TableQuery.CombineFilters(lsAzureFilter, TableOperators.And, lsPartitionKeyFilter);
                }
                else
                {
                    lsAzureFilter = lsPartitionKeyFilter;
                }
            }

            if (loData.DataModel is MaxIdGuidDataModel ||
                loData.DataModel is MaxIdIntegerDataModel ||
                loData.DataModel is MaxIdStringDataModel ||
                loData.DataModel is MaxBaseGuidKeyDataModel)
            {
                string lsRowKey = string.Empty;
                if (loData.DataModel is MaxIdGuidDataModel)
                {
                    Guid loId = MaxConvertLibrary.ConvertToGuid(loData.DataModel.GetType(), loData.Get(((MaxIdGuidDataModel)loData.DataModel).Id));
                    if (!Guid.Empty.Equals(loId))
                    {
                        lsRowKey = loId.ToString().ToLowerInvariant();
                    }
                }
                else if (loData.DataModel is MaxBaseGuidKeyDataModel)
                {
                    Guid loId = MaxConvertLibrary.ConvertToGuid(loData.DataModel.GetType(), loData.Get(((MaxBaseGuidKeyDataModel)loData.DataModel).Id));
                    if (!Guid.Empty.Equals(loId))
                    {
                        lsRowKey = loId.ToString().ToLowerInvariant();
                    }
                }
                else if (loData.DataModel is MaxIdIntegerDataModel)
                {
                    long lnId = MaxConvertLibrary.ConvertToLong(loData.DataModel.GetType(), loData.Get(((MaxIdIntegerDataModel)loData.DataModel).Id));
                    if (!long.MinValue.Equals(lnId))
                    {
                        lsRowKey = lnId.ToString().ToLowerInvariant();
                    }
                }
                else if (loData.DataModel is MaxIdStringDataModel)
                {
                    lsRowKey = MaxConvertLibrary.ConvertToString(loData.DataModel.GetType(), loData.Get(((MaxIdStringDataModel)loData.DataModel).Id)).ToLowerInvariant();
                }

                if (!string.Empty.Equals(lsRowKey))
                {
                    string lsRowKeyFilter = TableQuery.GenerateFilterCondition(
                        "RowKey",
                        QueryComparisons.Equal,
                        lsRowKey);

                    if (lsAzureFilter.Length > 0)
                    {
                        lsAzureFilter = TableQuery.CombineFilters(lsAzureFilter, TableOperators.And, lsRowKeyFilter);
                    }
                    else
                    {
                        lsAzureFilter = lsRowKeyFilter;
                    }
                }
            }

            loQuery.FilterString = lsAzureFilter;
            return loQuery;
        }

        /// <summary>
        /// Gets the filter element.
        /// </summary>
        /// <param name="lsName">Column name in the table.</param>
        /// <param name="lsOperator">Operator to compare the value.</param>
        /// <param name="loValue">The value to compare.</param>
        /// <param name="loDataModel">The definition of the data.</param>
        /// <returns>String filter element.</returns>
        public static string GetFilterCondition(string lsName, string lsOperator, object loValue, MaxDataModel loDataModel)
        {
            string lsOperation = string.Empty;
            if (lsOperator.Equals("="))
            {
                lsOperation = QueryComparisons.Equal;
            }
            else if (lsOperator.Equals(">"))
            {
                lsOperation = QueryComparisons.GreaterThan;
            }
            else if (lsOperator.Equals(">="))
            {
                lsOperation = QueryComparisons.GreaterThanOrEqual;
            }
            else if (lsOperator.Equals("<"))
            {
                lsOperation = QueryComparisons.LessThan;
            }
            else if (lsOperator.Equals("<="))
            {
                lsOperation = QueryComparisons.LessThanOrEqual;
            }
            else if (lsOperator.Equals("!=") || lsOperator.Equals("<>"))
            {
                lsOperation = QueryComparisons.NotEqual;
            }

            string lsR = string.Empty;
            if (loValue.GetType() == typeof(Guid))
            {
                lsR = TableQuery.GenerateFilterConditionForGuid(
                    lsName,
                    QueryComparisons.Equal,
                    (Guid)loValue);
            }
            else if (loValue.GetType() == typeof(bool))
            {
                lsR = TableQuery.GenerateFilterConditionForBool(
                    lsName,
                    lsOperation,
                    (bool)loValue);
            }
            else if (loValue.GetType() == typeof(DateTime))
            {
                lsR = TableQuery.GenerateFilterConditionForDate(
                    lsName,
                    lsOperation,
                    (DateTime)loValue);
            }
            else if (loValue.GetType() == typeof(double))
            {
                lsR = TableQuery.GenerateFilterConditionForDouble(
                    lsName,
                    lsOperation,
                    (double)loValue);
            }
            else if (loValue.GetType() == typeof(int))
            {
                lsR = TableQuery.GenerateFilterConditionForInt(
                    lsName,
                    lsOperation,
                    (int)loValue);
            }
            else if (loValue.GetType() == typeof(long))
            {
                lsR = TableQuery.GenerateFilterConditionForLong(
                    lsName,
                    lsOperation,
                    (long)loValue);
            }
            else if (loValue.GetType() == typeof(string))
            {
                lsR = TableQuery.GenerateFilterCondition(
                    lsName,
                    lsOperation,
                    (string)loValue);
            }

            return lsR;
        }
    }
}