// <copyright file="MaxDataContextAzureStorageProvider.cs" company="Lakstins Family, LLC">
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
// <change date="2/26/2014" author="Brian A. Lakstins" description="Initial Release">
// <change date="6/17/2014" author="Brian A. Lakstins" description="Update for base method rename.  Add relation key information.  Move functionality into a static method library.">
// <change date="6/23/2014" author="Brian A. Lakstins" description="Update query functionality.">
// <change date="6/24/2014" author="Brian A. Lakstins" description="Updateto make sure property exists before trying to map it.">
// <change date="8/13/2014" author="Brian A. Lakstins" description="Added logging.">
// <change date="8/21/2014" author="Brian A. Lakstins" description="Moved from AzureTable to AzureStorage.  Added Stream support.">
// <change date="9/16/2014" author="Brian A. Lakstins" description="Added information diagnostic timing for all data retrieval and changes.">
// <change date="11/10/2014" author="Brian A. Lakstins" description="Updated for changes to core.">
// <change date="6/8/2015" author="Brian A. Lakstins" description="Add more log details.">
// <change date="1/6/2016" author="Brian A. Lakstins" description="Store long text as a stream.">
// <change date="1/28/2015" author="Brian A. Lakstins" description="Fix using name for getting configuration.">
// <change date="7/4/2016" author="Brian A. Lakstins" description="Updated to access provider configuration using base provider methods.">
// <change date="7/12/2016" author="Brian A. Lakstins" description="Add ability to specify name of file to use in url for stored stream.">
// <change date="12/21/2016" author="Brian A. Lakstins" description="Update to save properties as stream if they might be too large for table storage.">
// <change date="7/7/2019" author="Brian A. Lakstins" description="Fix issue with storing external data when it has not changed.">
// <change date="10/23/2019" author="Brian A. Lakstins" description="Updates save public files to a separate container.">
// <change date="12/11/2019" author="Brian A. Lakstins" description="Updated so can pull all records with a PartitionKey prefix so can pull all records based on a storagekey even if each records use a suffix.">
// <change date="6/5/2020" author="Brian A. Lakstins" description="Updated for change to base.">
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
    public class MaxDataContextAzureStorageProvider : MaxProvider, IMaxDataContextProvider
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
        /// List of tables that have been created.
        /// </summary>
        private MaxIndex _oTableList = new MaxIndex();

        /// <summary>
        /// Container to use for blob storage.
        /// </summary>
        private string _sContainer = string.Empty;

        /// <summary>
        /// Container to use for blob storage.
        /// </summary>
        private string _sCdn = string.Empty;

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

            string lsCdn = this.GetConfigValue(loConfig, "AzureCDN") as string;
            if (null != lsCdn)
            {
                this._sCdn = lsCdn;
            }


            System.Net.ServicePointManager.UseNagleAlgorithm = false;
        }

        /// <summary>
        /// Selects data from the database.
        /// </summary>
        /// <param name="loData">Element with data used in the filter.</param>
        /// <param name="loDataQuery">Query information to filter results.</param>
        /// <param name="lnPageIndex">Page to return.</param>
        /// <param name="lnPageSize">Items per page.</param>
        /// <param name="lsSort">Sort information.</param>
        /// <param name="lnTotal">Total items found.</param>
        /// <param name="laFields">list of fields to return from select.</param>
        /// <returns>List of data from select.</returns>
        public MaxDataList Select(MaxData loData, MaxDataQuery loDataQuery, int lnPageIndex, int lnPageSize, string lsSort, out int lnTotal, params string[] laFields)
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
                MaxDataList loDataAllList = MaxAzureTableLibrary.Select(this._sAccountName, this._sAccountKey, this._sContainer, loQuery, loData, lnPageIndex, lnPageSize, out lnTotal);
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
                loDataList = MaxAzureTableLibrary.Select(this._sAccountName, this._sAccountKey, this._sContainer, loQuery, loData, lnPageIndex, lnPageSize, out lnTotal);
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
        public int SelectCount(MaxData loData, MaxDataQuery loDataQuery)
        {
            string[] laFields = new string[] { "RowKey" };
            TableQuery loQuery = MaxAzureTableLibrary.GetTableQueryForSelect(loData, loDataQuery, laFields);
            CloudTableClient loTableClient = MaxAzureTableLibrary.GetTableClient(this._sAccountName, this._sAccountKey, loData.DataModel.DataStorageName);
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
        public int Insert(MaxDataList loDataList)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageInsertStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "insert {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
            int lnR = MaxAzureTableLibrary.Insert(this._sAccountName, this._sAccountKey, loDataList);
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
        public int Update(MaxDataList loDataList)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageUpdateStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "update {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
            int lnR = MaxAzureTableLibrary.Update(this._sAccountName, this._sAccountKey, loDataList);
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
        public int Delete(MaxDataList loDataList)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageDeleteStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete {DataStorageName} for {DataCount}", loDataList.DataModel.DataStorageName, loDataList.Count));
            int lnR = MaxAzureTableLibrary.Delete(this._sAccountName, this._sAccountKey, loDataList);
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
        public MaxDataList SelectAll(string lsDataStorageName, params string[] laFields)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageSelectAllStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "select all {DataStorageName}", lsDataStorageName));
            MaxDataList loList = MaxAzureTableLibrary.SelectAll(this._sAccountName, this._sAccountKey, lsDataStorageName);
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
        public virtual bool StreamSave(MaxData loData, string lsKey)
        {
            bool lbR = false;
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "save stream {Container} for {Key}", this._sContainer, lsKey));
            MaxIdGuidDataModel loDataModel = loData.DataModel as MaxIdGuidDataModel;
            if (null != loDataModel)
            {
                object loValueType = loData.DataModel.GetValueType(lsKey);
                if (loData.GetIsChanged(lsKey))
                {
                    //// Check defined storage type
                    if (typeof(MaxLongString).Equals(loValueType) || typeof(byte[]).Equals(loValueType) || typeof(Stream).Equals(loValueType))
                    {
                        object loValue = loData.Get(lsKey);
                        if (null != loValue && (loValue is Stream || loValue is string || loValue is byte[]))
                        {
                            string lsContentType = "application/octet-stream";
                            Stream loStream = null;
                            if (loValue is string && ((string)loValue).StartsWith(MaxAzureTableLibrary.AzureStringStreamIndicator))
                            {
                                lsContentType = "text/plain";
                                string lsValue = ((string)loValue).Substring(MaxAzureTableLibrary.AzureStringStreamIndicator.Length);
                                loData.Set(lsKey, lsValue);
                                loStream = new MemoryStream(System.Text.UTF8Encoding.UTF8.GetBytes(lsValue));
                            }
                            else if (loValue is string && ((string)loValue).Length > 16384)
                            {
                                //// Store as stream if over 16K in length
                                lsContentType = "text/plain";
                                loData.Set(lsKey, MaxDataModel.StreamStringIndicator);
                                loStream = new MemoryStream(System.Text.UTF8Encoding.UTF8.GetBytes(((string)loValue)));
                            }
                            else if (loValue is byte[] && ((byte[])loValue).Length > 16384)
                            {
                                //// Store as stream if over 16K in length
                                loData.Set(lsKey, MaxDataModel.StreamByteIndicator);
                                loStream = new MemoryStream((byte[])loValue);
                            }
                            else if (loValue is Stream)
                            {
                                loStream = (Stream)loValue;
                            }

                            if (null != loStream)
                            {
                                string lsContentTypeKey = lsKey + "Type";
                                object loContentType = loData.Get(lsContentTypeKey);
                                if (null != loContentType)
                                {
                                    lsContentType = MaxConvertLibrary.ConvertToString(typeof(object), loContentType);
                                }

                                int lnTry = 0;
                                string lsStreamFileName = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsKey);
                                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogDebug, "saving stream {StreamFileName}", lsStreamFileName));
                                while (!lbR && lnTry < 3)
                                {
                                    lbR = MaxAzureBlobLibrary.StreamSave(
                                        this._sAccountName,
                                        this._sAccountKey,
                                        this._sContainer.ToLowerInvariant(),
                                        lsStreamFileName,
                                        loStream,
                                        lsContentType,
                                        true);
                                    lnTry++;
                                    System.Threading.Thread.Sleep(100);
                                }

                                if (lnTry >= 3)
                                {
                                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogError, "saving stream {StreamFileName} failed after {Try}", lsStreamFileName, lnTry));
                                }
                                else if (lnTry > 1)
                                {
                                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogWarning, "saving stream {StreamFileName} took multiple tries {Try}", lsStreamFileName, lnTry));
                                }

                                loData.ClearChanged(lsKey);
                            }
                        }
                    }
                }
                else if (typeof(MaxLongString).Equals(loValueType) || typeof(byte[]).Equals(loValueType) || typeof(Stream).Equals(loValueType))
                {
                    object loValue = loData.Get(lsKey);
                    if (null != loValue && (loValue is Stream || loValue is string || loValue is byte[]))
                    {
                        if (loValue is string && ((string)loValue).StartsWith(MaxAzureTableLibrary.AzureStringStreamIndicator))
                        {
                            string lsValue = ((string)loValue).Substring(MaxAzureTableLibrary.AzureStringStreamIndicator.Length);
                            loData.Set(lsKey, lsValue);
                        }
                        else if (loValue is string && ((string)loValue).Length > 16384)
                        {
                            //// Store as stream if over 16K in length
                            loData.Set(lsKey, MaxDataModel.StreamStringIndicator);
                        }
                        else if (loValue is byte[] && ((byte[])loValue).Length > 16384)
                        {
                            //// Store as stream if over 16K in length
                            loData.Set(lsKey, MaxDataModel.StreamByteIndicator);
                        }
                    }
                }
            }

            /* This saves the stream as a file that can be accessed by Url
             * There has to be a DataModel key that has some other keys associated with it to provide at least the name of the file.
             * Key - Stream
             * KeyName - String
             * KeyType - String - optional
             * 
             * For MaxBaseIdFile (which is based on MaxBaseIdVersioned, so it has versions) these properties are
             * Content - Stream
             * ContentName - String
             * ContentType - String (uses Mime Type to determine ContentType)
             * 
             * These streams are saved in a way that they can be accessed through a URL.
             * The Url can be determined using GetStreamUrl
             *
             */
            string lsKeyName = lsKey + "Name";
            if (null != loDataModel && loData.GetIsChanged(lsKeyName))
            {
                string lsName = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(lsKeyName));
                if (!string.IsNullOrEmpty(lsName))
                {
                    string lsContentType = "application/octet-stream";
                    string lsContentTypeKey = lsKey + "Type";
                    object loContentType = loData.Get(lsContentTypeKey);
                    if (null != loContentType)
                    {
                        lsContentType = MaxConvertLibrary.ConvertToString(typeof(object), loContentType);
                    }

                    Stream loStreamName = this.StreamOpen(loData, lsKey);
                    if (null != loStreamName)
                    {
                        int lnTry = 0;
                        lbR = false;
                        string lsStreamFileNamePublic = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsName);
                        MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogDebug, "saving public stream {StreamFileName}", lsStreamFileNamePublic));
                        while (!lbR && lnTry < 3)
                        {
                            lbR = MaxAzureBlobLibrary.StreamSave(
                                this._sAccountName,
                                this._sAccountKey,
                                this._sContainer.ToLowerInvariant() + "-public",
                                lsStreamFileNamePublic,
                                loStreamName,
                                lsContentType,
                                false);
                            lnTry++;
                            System.Threading.Thread.Sleep(100);
                        }

                        if (lnTry >= 3)
                        {
                            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogError, "saving public stream {StreamFileName} failed after {Try}", lsStreamFileNamePublic, lnTry));
                        }
                        else if (lnTry > 1)
                        {
                            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogWarning, "saving public stream {StreamFileName} took multiple tries {Try}", lsStreamFileNamePublic, lnTry));
                        }
                    }
                }
            }

            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "save stream {Container} for {Key} in {Milliseconds}", this._sContainer, lsKey, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "save stream {Container} for {Key} in {Milliseconds}", this._sContainer, lsKey, loWatch.Elapsed.TotalMilliseconds));
            }

            return lbR;
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public virtual Stream StreamOpen(MaxData loData, string lsKey)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            if (loData.DataModel is MaxIdGuidDataModel)
            {
                MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                return MaxAzureBlobLibrary.StreamOpen(
                            this._sAccountName,
                            this._sAccountKey,
                            this._sContainer.ToLowerInvariant(),
                            MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsKey));
            }

            return null;
        }

        /// <summary>
        /// Deletes a stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public virtual bool StreamDelete(MaxData loData, string lsKey)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            if (loData.DataModel is MaxIdGuidDataModel)
            {
                MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                string lsStorageLocation = this._sContainer.ToLowerInvariant();

                // Create the blob client.
                CloudBlobClient loBlobClient = new CloudStorageAccount(new StorageCredentials(this._sAccountName, this._sAccountKey), true).CreateCloudBlobClient();

                // Retrieve a reference to a container.
                CloudBlobContainer loContainer = loBlobClient.GetContainerReference(lsStorageLocation);

                if (loContainer.Exists())
                {
                    object loId = loData.Get(loDataModel.Id);
                    string lsStorageKey = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(loData.DataModel.StorageKey));
                    string lsFileName = lsStorageKey + "/" + loData.DataModel.DataStorageName + "/" + loId.ToString() + "_" + lsKey;

                    // Retrieve reference to the blob based on the key.
                    CloudBlockBlob loBlockBlob = loContainer.GetBlockBlobReference(lsFileName);
                    if (loBlockBlob.Exists())
                    {
                        loBlockBlob.Delete();
                        return true;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Gets the Url to use to access the stream.
        /// </summary>
        /// <param name="loData">Data used to help determine url.</param>
        /// <param name="lsKey">Key used to help determine key.</param>
        /// <returns>Url to access the stream.</returns>
        public virtual string GetStreamUrl(MaxData loData, string lsKey)
        {
            string lsR = string.Empty;
            if (loData.DataModel is MaxIdGuidDataModel)
            {
                MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                Guid loId = MaxConvertLibrary.ConvertToGuid(typeof(object), loData.Get(loDataModel.Id));
                if (Guid.Empty != loId)
                {
                    string lsKeyName = lsKey + "Name";
                    string lsName = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(lsKeyName));
                    if (!string.IsNullOrEmpty(lsName))
                    {
                        string lsFile = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsName);
                        string lsStorageLocation = this._sContainer.ToLowerInvariant();
                        string lsFileUrl = string.Empty;
                        // Create the blob client.
                        CloudBlobClient loBlobClient = new CloudStorageAccount(new StorageCredentials(this._sAccountName, this._sAccountKey), true).CreateCloudBlobClient();

                        // Retrieve a reference to the public container.
                        CloudBlobContainer loContainer = loBlobClient.GetContainerReference(lsStorageLocation + "-public");
                        if (!loContainer.Exists())
                        {
                            //// Save the stream to public
                            Stream loStream = this.StreamOpen(loData, lsKey);
                            if (null != loStream)
                            {
                                loData.Set(lsKey, loStream);
                                loData.SetChanged();
                                StreamSave(loData, lsKey);
                                lsFileUrl = lsFile;
                            }
                        }
                        else
                        {
                            CloudBlockBlob loBlockBlob = loContainer.GetBlockBlobReference(lsFile);
                            if (!loBlockBlob.Exists())
                            {
                                //// Save the stream to public
                                Stream loStream = this.StreamOpen(loData, lsKey);
                                if (null != loStream)
                                {
                                    loData.Set(lsKey, loStream);
                                    loData.SetChanged();
                                    StreamSave(loData, lsKey);
                                    lsFileUrl = lsFile;
                                }
                            }
                            else
                            {
                                lsFileUrl = lsFile;
                            }
                        }

                        string lsBaseUrl = string.Format("{0}.blob.core.windows.net", this._sAccountName);
                        if (!string.IsNullOrEmpty(this._sCdn))
                        {
                            lsBaseUrl = string.Format("{0}", this._sCdn);
                        }

                        if (!string.IsNullOrEmpty(lsFileUrl))
                        {
                            lsR = string.Format("//{0}/{1}/{2}", lsBaseUrl, lsStorageLocation + "-public", lsFileUrl);
                        }
                    }
                }
            }

            return lsR;
        }

        /// <summary>
        /// Enables Cross-Origin Resource Sharing
        /// </summary>
        /// <param name="lsOrigin">Origin to allow.  * for all sites.</param>
        public void EnableCors(string lsOrigin)
        {
            // Create the blob client.
            CloudBlobClient loBlobClient = new CloudStorageAccount(new StorageCredentials(this._sAccountName, this._sAccountKey), true).CreateCloudBlobClient();

            // Get the service properties
            ServiceProperties loServiceProperties = loBlobClient.GetServiceProperties();

            CorsRule loRule = new CorsRule();
            loRule.AllowedOrigins.Add(lsOrigin);
            loRule.AllowedMethods = CorsHttpMethods.Get;
            loRule.MaxAgeInSeconds = 3600;

            loServiceProperties.Cors.CorsRules.Add(loRule);
            loBlobClient.SetServiceProperties(loServiceProperties);
        }
    }
}