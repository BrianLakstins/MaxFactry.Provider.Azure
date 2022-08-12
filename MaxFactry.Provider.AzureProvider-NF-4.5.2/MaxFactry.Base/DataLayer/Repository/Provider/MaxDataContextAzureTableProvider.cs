// <copyright file="MaxDataContextAzureTableProvider.cs" company="Lakstins Family, LLC">
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
// <change date="5/29/2021" author="Brian A. Lakstins" description="Initial Creation based on MaxDataContextAzureStorageProvider">
// </changelog>
#endregion Change Log

namespace MaxFactry.Base.DataLayer.Provider
{
    using System;
    using System.IO;
    using MaxFactry.Core;
    using MaxFactry.Provider.AzureProvider.DataLayer;
    using MaxFactry.Base.DataLayer;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Provides session services using MaxFactryLibrary.
    /// </summary>
    public class MaxDataContextAzureTableProvider : MaxDataContextAzureSqlProvider
    {
        /// <summary>
        /// Initializes the provider
        /// </summary>
        /// <param name="lsName">Name of the provider</param>
        /// <param name="loConfig">Configuration information</param>
        public override void Initialize(string lsName, MaxIndex loConfig)
        {
            base.Initialize(lsName, loConfig);
        }

        /// <summary>
        /// Selects data from the database.
        /// </summary>
        /// <param name="loData">Element with data used in the filter.</param>
        /// <param name="loDataQuery">Query information to filter results.</param>
        /// <param name="lnPageIndex">Page to return.</param>
        /// <param name="lnPageSize">Items per page.</param>
        /// <param name="lsSort">Sort information</param>
        /// <param name="lnTotal">Total items found.</param>
        /// <param name="laFields">list of fields to return from select.</param>
        /// <returns>List of data from select.</returns>
        public override MaxDataList Select(MaxData loData, MaxDataQuery loDataQuery, int lnPageIndex, int lnPageSize, string lsSort, out int lnTotal, params string[] laFields)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageSelectStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "select {DataStorageName}", loData.DataModel.DataStorageName));
            TableQuery loQuery = MaxAzureTableLibrary.GetTableQueryForSelect(loData, loDataQuery, laFields);
            MaxDataList loDataList = new MaxDataList(loData.DataModel);
            //// Special case to select all records that have a PartitionKey that starts with the storage key instead of just the ones that match it exactly.
            if (lnPageSize == -1)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageSelect", MaxFactry.Core.MaxEnumGroup.LogDebug, "Special case {DataStorageName}", loData.DataModel.DataStorageName));
                //// Set to select all matching records regardless of storage key
                loData.Set(loData.DataModel.StorageKey, "*");

                //// Determine the storage key to match to the first part of the Partition Key
                string lsStorageKey = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(loData.DataModel.StorageKey));
                if (string.IsNullOrEmpty(lsStorageKey))
                {
                    lsStorageKey = MaxAzureTableLibrary.DefaultPartitionKey;
                }

                loQuery = MaxAzureTableLibrary.GetTableQueryForSelect(loData, loDataQuery, laFields);
                MaxDataList loDataAllList = MaxAzureTableLibrary.Select(this.AccountName, this.AccountKey, this.Container, loQuery, loData, lnPageIndex, lnPageSize, out lnTotal);
                //// Filter out all records that don't start with the Storage Key
                for (int lnD = 0; lnD < loDataAllList.Count; lnD++)
                {
                    string lsPartitionKey = loDataAllList[lnD].Get("_PartitionKey") as string;
                    if (null != lsPartitionKey && lsPartitionKey.Length > 0 && lsPartitionKey.StartsWith(lsStorageKey))
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
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageSelectEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "select {DataStorageName} for {RowCount} took {Milliseconds}", loData.DataModel.DataStorageName, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageSelectEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "select {DataStorageName} for {RowCount} took {Milliseconds}", loData.DataModel.DataStorageName, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }

            return loDataList;
        }

        /// <summary>
        /// Gets the number of records that match the filter.
        /// </summary>
        /// <param name="loData">Element with data used in the filter.</param>
        /// <param name="loDataQuery">Query information to filter results.</param>
        /// <returns>number of records that match.</returns>
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
        /// Inserts a new data element.
        /// </summary>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>The data that was inserted.</returns>
        public override int Insert(MaxDataList loDataList)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageInsertStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
            int lnR = MaxAzureTableLibrary.Insert(this.AccountName, this.AccountKey, loDataList);
            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 100)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageInsertEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "insert {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageInsertEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }

            return lnR;
        }

        /// <summary>
        /// Updates an existing data element.
        /// </summary>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>The data that was updated.</returns>
        public override int Update(MaxDataList loDataList)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageUpdateStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
            int lnR = MaxAzureTableLibrary.Update(this.AccountName, this.AccountKey, loDataList);
            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 100)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageUpdateEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "update {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageUpdateEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }

            return lnR;
        }

        /// <summary>
        /// Deletes an existing data element.
        /// </summary>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>true if deleted.</returns>
        public override int Delete(MaxDataList loDataList)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageDeleteStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
            int lnR = MaxAzureTableLibrary.Delete(this.AccountName, this.AccountKey, loDataList);
            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 100)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageDeleteEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "delete {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageDeleteEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {DataStorageName} {RowCount} for {DataCount} in {Milliseconds}", loDataList.DataModel.DataStorageName, lnR, loDataList.Count, loWatch.Elapsed.TotalMilliseconds));
            }

            return lnR;
        }

        /// <summary>
        /// Selects all data from the data storage name for the specified type.
        /// </summary>
        /// <param name="lsDataStorageName">Name of the data storage (table name).</param>
        /// <param name="laFields">list of fields to return from select</param>
        /// <returns>List of data elements with a base data model.</returns>
        public override MaxDataList SelectAll(string lsDataStorageName, params string[] laFields)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageSelectAllStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "select all {DataStorageName}", lsDataStorageName));
            MaxDataList loList = MaxAzureTableLibrary.SelectAll(this.AccountName, this.AccountKey, lsDataStorageName);
            MaxFactry.Core.MaxLogLibrary.Log(MaxFactry.Core.MaxEnumGroup.LogInfo, "Select All [" + lsDataStorageName + "] in [" + loWatch.ElapsedMilliseconds.ToString() + "] milliseconds.", "MaxAzureTableDataContextProvider");
            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageSelectAllEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "select all {DataStorageName} {RowCount} in {Milliseconds}", lsDataStorageName, loList.Count, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageSelectAllEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "select all {DataStorageName} {RowCount} in {Milliseconds}", lsDataStorageName, loList.Count, loWatch.Elapsed.TotalMilliseconds));
            }

            return loList;
        }

        /// <summary>
        /// Writes stream data to storage.
        /// https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Understanding-the-Table-Service-Data-Model?redirectedfrom=MSDN
        /// An entity can have up to 255 properties, including 3 system properties described in the following section. 
        /// Therefore, the user may include up to 252 custom properties, in addition to the 3 system properties. 
        /// The combined size of all data in an entity's properties cannot exceed 1 MB.
        /// Edm.String	String	A UTF-16-encoded value. String values may be up to 64 KB in size
        /// Edm.Binary	byte[]	An array of bytes up to 64 KB in size.
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Number of bytes written to storage.</returns>
        public override bool StreamSave(MaxData loData, string lsKey)
        {
            return MaxDataStreamAzureBlobLibrary.StreamSave(loData, lsKey, this.Container, this.AccountName, this.AccountKey, 16384, 16384);
        }
    }
}