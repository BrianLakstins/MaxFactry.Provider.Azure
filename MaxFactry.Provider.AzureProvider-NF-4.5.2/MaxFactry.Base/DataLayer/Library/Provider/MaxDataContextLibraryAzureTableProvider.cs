// <copyright file="MaxDataContextLibraryAzureTableProvider.cs" company="Lakstins Family, LLC">
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
// <change date="5/29/2021" author="Brian A. Lakstins" description="Initial Creation based on MaxDataContextAzureTableProvider">
// <change date="3/31/2024" author="Brian A. Lakstins" description="Updated namespace and class name to match MaxFactry.Base naming conventions.">
// <change date="5/23/2025" author="Brian A. Lakstins" description="Remove stream handling methods and integrate stream handling using StreamLibrary">
// </changelog>
#endregion Change Log

namespace MaxFactry.Base.DataLayer.Library.Provider
{
    using MaxFactry.Core;
    using MaxFactry.Provider.AzureProvider.DataLayer;
    using MaxFactry.Base.DataLayer;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Data Context used to work with data in Azure Tables and Stream storage on Azure Blob
    /// </summary>
    public class MaxDataContextLibraryAzureTableProvider : MaxDataContextLibraryDefaultProvider
    {
        /// <summary>
        /// The account name to connect to the Azure Table Storage service
        /// </summary>
        private string _sAccountName = string.Empty;

        /// <summary>
        /// The account key to connect to the Azure Table Storage service.
        /// </summary>
        private string _sAccountKey = string.Empty;

        /// <summary>
        /// Container to use for blob storage.
        /// </summary>
        private string _sContainer = string.Empty;

        /// <summary>
        /// Initializes the provider
        /// </summary>
        /// <param name="lsName">Name of the provider</param>
        /// <param name="loConfig">Configuration information</param>
        public override void Initialize(string lsName, MaxIndex loConfig)
        {
            base.Initialize(lsName, loConfig);
            string lsAccountName = this.GetConfigValue(loConfig, "AzureAccountName") as string;
            if (null != lsAccountName)
            {
                this._sAccountName = lsAccountName;
            }

            string lsAccountKey = this.GetConfigValue(loConfig, "AzureAccountKey") as string;
            if (null != lsAccountKey)
            {
                this._sAccountKey = lsAccountKey;
            }

            string lsContainer = this.GetConfigValue(loConfig, "AzureContainer") as string;
            if (null != lsContainer)
            {
                this._sContainer = lsContainer;
            }

            System.Net.ServicePointManager.UseNagleAlgorithm = false;
        }

        protected string AccountName
        {
            get
            {
                return this._sAccountName;
            }
        }

        protected string AccountKey
        {
            get
            {
                return this._sAccountKey;
            }
        }

        protected string Container
        {
            get
            {
                return this._sContainer;
            }
        }

        /// <summary>
        /// Selects all data
        /// </summary>
        /// <param name="loData">Data to use as definition</param>
        /// <param name="laDataNameList">Names of fields to return</param>
        /// <returns>List of data that is stored</returns>
        public override MaxDataList SelectAll(MaxData loData, params string[] laDataNameList)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableSelectAllStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "select all {DataStorageName}", loData.DataModel.DataStorageName));
            MaxDataList loList = MaxAzureTableLibrary.SelectAll(this.AccountName, this.AccountKey, loData);
            MaxFactry.Core.MaxLogLibrary.Log(MaxFactry.Core.MaxEnumGroup.LogInfo, "Select All [" + loData.DataModel.DataStorageName + "] in [" + loWatch.ElapsedMilliseconds.ToString() + "] milliseconds.", "MaxAzureTableDataContextProvider");
            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableSelectAllEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "select all {DataStorageName} {RowCount} in {Milliseconds}", loData.DataModel.DataStorageName, loList.Count, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableSelectAllEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "select all {DataStorageName} {RowCount} in {Milliseconds}", loData.DataModel.DataStorageName, loList.Count, loWatch.Elapsed.TotalMilliseconds));
            }

            return loList;
        }

        /// <summary>
        /// Selects data
        /// </summary>
        /// <param name="loData">Data to use as definition</param>
        /// <param name="loDataQuery">Filter for the query</param>
        /// <param name="lnPageIndex">Page number of the data</param>
        /// <param name="lnPageSize">Size of the page</param>
        /// <param name="lsOrderBy">Data field used to sort</param>
        /// <param name="laDataNameList">Names of fields to return</param>
        /// <returns>List of data that matches the query parameters</returns>
        public override MaxDataList Select(MaxData loData, MaxDataQuery loDataQuery, int lnPageIndex, int lnPageSize, string lsOrderBy, out int lnTotal, params string[] laDataNameList)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableSelectStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "select {DataStorageName}", loData.DataModel.DataStorageName));
            TableQuery loQuery = MaxAzureTableLibrary.GetTableQueryForSelect(loData, loDataQuery, laDataNameList);
            MaxDataList loDataList = new MaxDataList(loData.DataModel);
            //// Special case to select all records that have a PartitionKey that starts with the storage key instead of just the ones that match it exactly.
            if (lnPageSize == -1)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableSelect", MaxFactry.Core.MaxEnumGroup.LogDebug, "Special case {DataStorageName}", loData.DataModel.DataStorageName));
                //// Set to select all matching records regardless of storage key
                MaxBaseDataModel loBaseDataModel = loData.DataModel as MaxBaseDataModel;
                if (loBaseDataModel != null)
                {
                    loData.Set(loBaseDataModel.StorageKey, "*");
                }

                //// Determine the storage key to match to the first part of the Partition Key
                string lsStorageKey = loData.DataModel.GetStorageKey(loData);
                if (loBaseDataModel != null)
                {
                    lsStorageKey = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(loBaseDataModel.StorageKey));
                    if (string.IsNullOrEmpty(lsStorageKey))
                    {
                        //// Use the dynamic storage key from the application configuration if it is not set.
                        lsStorageKey = MaxDataLibrary.GetStorageKey(null);
                        if (string.IsNullOrEmpty(lsStorageKey))
                        {
                            //// If the storage key is still not set, use the default partition key.
                            lsStorageKey = MaxAzureTableLibrary.DefaultPartitionKey;
                        }
                    }
                }

                loQuery = MaxAzureTableLibrary.GetTableQueryForSelect(loData, loDataQuery, laDataNameList);
                MaxDataList loDataAllList = MaxAzureTableLibrary.Select(this.AccountName, this.AccountKey, this.Container, loQuery, loData, lnPageIndex, lnPageSize, out lnTotal);
                //// Filter out all records that don't start with the Storage Key
                for (int lnD = 0; lnD < loDataAllList.Count; lnD++)
                {
                    string lsPartitionKey = loDataAllList[lnD].Get("_PartitionKey") as string;
                    if (null != lsPartitionKey && lsPartitionKey.Length > 0 && (!string.IsNullOrEmpty(lsStorageKey) && lsPartitionKey.StartsWith(lsStorageKey)))
                    {
                        MaxData loDataMatch = loDataAllList[lnD].Clone();
                        loDataList.Add(loDataMatch);
                    }
                }
            }
            else
            {
                loDataList = MaxAzureTableLibrary.Select(this.AccountName, this.AccountKey, this.Container, loQuery, loData, lnPageIndex, lnPageSize, out lnTotal);
            }

            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableSelectEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "select {DataStorageName} for {RowCount} took {Milliseconds}", loData.DataModel.DataStorageName, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableSelectEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "select {DataStorageName} for {RowCount} took {Milliseconds}", loData.DataModel.DataStorageName, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }

            return loDataList;
        }

        /// <summary>
        /// Selects a count of records
        /// </summary>
        /// <param name="loData">Data to use as definition</param>
        /// <param name="loDataQuery">Filter for the query</param>
        /// <returns>Count that matches the query parameters</returns>
        public override int SelectCount(MaxData loData, MaxDataQuery loDataQuery)
        {
            string[] laFields = new string[] { "RowKey" };
            TableQuery loQuery = MaxAzureTableLibrary.GetTableQueryForSelect(loData, loDataQuery, laFields);
            CloudTableClient loTableClient = MaxAzureTableLibrary.GetTableClient(this.AccountName, this.AccountKey, loData.DataModel.DataStorageName);
            CloudTable loTable = loTableClient.GetTableReference(loData.DataModel.DataStorageName);
            int lnRows = 0;
            foreach (DynamicTableEntity loDataEntity in loTable.ExecuteQuery(loQuery))
            {
                lnRows++;
            }

            return lnRows;
        }

        /// <summary>
        /// Inserts a new list of elements
        /// </summary>
        /// <param name="loDataList">The list of elements</param>
        /// <returns>Flag based status code indicating level of success.</returns>
        public override int Insert(MaxDataList loDataList)
        {
            int lnR = 0;
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            for (int lnD = 0; lnD < loDataList.Count && lnR == 0; lnD++)
            {
                MaxData loData = loDataList[lnD];
                foreach (string lsDataName in loData.DataModel.DataNameStreamList)
                {
                    int lnReturn = MaxStreamLibrary.StreamSave(loData, lsDataName);
                    if ((lnReturn & 1) != 0)
                    {
                        lnR |= 2; //// Error saving stream
                    }
                }
            }

            if (lnR == 0)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsertStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
                lnR = MaxAzureTableLibrary.Insert(this.AccountName, this.AccountKey, loDataList);
                loWatch.Stop();
                if (loWatch.Elapsed.TotalMilliseconds > 100)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsertEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "insert {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
                }
                else
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableInsertEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
                }
            }

            return lnR;
        }

        /// <summary>
        /// Updates a list of elements
        /// </summary>
        /// <param name="loDataList">The list of elements</param>
        /// <returns>Flag based status code indicating level of success.</returns>
        public override int Update(MaxDataList loDataList)
        {
            int lnR = 0;
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            for (int lnD = 0; lnD < loDataList.Count && lnR == 0; lnD++)
            {
                MaxData loData = loDataList[lnD];
                foreach (string lsDataName in loData.DataModel.DataNameStreamList)
                {
                    int lnReturn = MaxStreamLibrary.StreamSave(loData, lsDataName);
                    if ((lnReturn & 1) != 0)
                    {
                        lnR |= 2; //// Error saving stream
                    }
                }
            }

            if (lnR == 0)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
                lnR = MaxAzureTableLibrary.Update(this.AccountName, this.AccountKey, loDataList);
                loWatch.Stop();
                if (loWatch.Elapsed.TotalMilliseconds > 100)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "update {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
                }
                else
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableUpdateEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
                }
            }

            return lnR;
        }

        /// <summary>
        /// Deletes a list of elements
        /// </summary>
        /// <param name="loDataList">The list of elements</param>
        /// <returns>Flag based status code indicating level of success.</returns>
        public override int Delete(MaxDataList loDataList)
        {
            int lnR = 0;
            for (int lnD = 0; lnD < loDataList.Count; lnD++)
            {
                MaxData loData = loDataList[lnD];
                foreach (string lsDataName in loData.DataModel.DataNameStreamList)
                {
                    int lnReturn = MaxStreamLibrary.StreamDelete(loData, lsDataName);
                    if ((lnReturn & 1) != 0)
                    {
                        lnR |= 2; //// Error deleting stream
                    }
                }
            }

            if (lnR == 0)
            {
                System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDeleteStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
                lnR = MaxAzureTableLibrary.Delete(this.AccountName, this.AccountKey, loDataList);
                loWatch.Stop();
                if (loWatch.Elapsed.TotalMilliseconds > 100)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDeleteEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "delete {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
                }
                else
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableDeleteEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
                }
            }

            return lnR;
        }
    }
}