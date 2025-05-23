// <copyright file="MaxStreamLibraryAzureBlobProvider.cs" company="Lakstins Family, LLC">
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
#endregion

#region Change Log
// <changelog>
// <change date="9/20/2023" author="Brian A. Lakstins" description="Initial creation">
// <change date="3/31/2024" author="Brian A. Lakstins" description="Updated namespace and class name to match MaxFactry.Base naming conventions.">
// <change date="5/23/2025" author="Brian A. Lakstins" description="Update to handle one field of one element at a time and send flag based return codes.  Integrate MaxDataStreamAzureBlobLibrary code instead of calling remotely.">
// </changelog>
#endregion

namespace MaxFactry.Base.DataLayer.Library.Provider
{
    using System;
    using System.IO;
    using MaxFactry.Base.DataLayer;
    using MaxFactry.Core;
    using MaxFactry.Provider.AzureProvider.DataLayer;
    using Microsoft.Azure.KeyVault.Core;

    /// <summary>
    /// Stream Library provider used to work with data on Azure and streams stored on Azure Blob
    /// </summary>
    public class MaxStreamLibraryAzureBlobProvider : MaxStreamLibraryDefaultProvider
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
        /// Container to use for blob storage.
        /// </summary>
        private string _sCdn = string.Empty;

        /// <summary>
        /// Max Length for a string to be in the database
        /// </summary>
        protected int _nMaxStringLength = 2000;

        /// <summary>
        /// Max Length for a byte to be in the database
        /// </summary>
        protected int _nMaxByteLength = 2000;

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

            string lsMaxStringLength = this.GetConfigValue(loConfig, "StreamMaxStringLength") as string;
            if (null != lsMaxStringLength)
            {
                this._nMaxStringLength = MaxConvertLibrary.ConvertToInt(typeof(object), lsMaxStringLength);
            }

            string lsMaxByteLength = this.GetConfigValue(loConfig, "StreamMaxByteLength") as string;
            if (null != lsMaxByteLength)
            {
                this._nMaxByteLength = MaxConvertLibrary.ConvertToInt(typeof(object), lsMaxByteLength);
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

        protected string Cdn
        {
            get
            {
                return this._sCdn;
            }
        }

        protected int MaxStringLength
        {
            get
            {
                return this._nMaxStringLength;
            }

            set
            {
                this._nMaxStringLength = value;
            }
        }

        protected int MaxByteLength
        {
            get
            {
                return this._nMaxByteLength;
            }

            set
            {
                this._nMaxByteLength = value;
            }
        }

        /// <summary>
        /// Writes stream data to blob storage when it can't be saved in a single field in table storage
        /// https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Understanding-the-Table-Service-Data-Model?redirectedfrom=MSDN
        /// An entity can have up to 255 properties, including 3 system properties described in the following section. 
        /// Therefore, the user may include up to 252 custom properties, in addition to the 3 system properties. 
        /// The combined size of all data in an entity's properties cannot exceed 1 MB.
        /// Edm.String	String	A UTF-16-encoded value. String values may be up to 64 KB in size
        /// Edm.Binary	byte[]	An array of bytes up to 64 KB in size.
        /// </summary>
        /// <param name="loData">The data element</param>
        /// <param name="lsDataName">Name of data element to save</param>
        /// <returns>Flag based status code indicating level of success.</returns>
        public override int StreamSave(MaxData loData, string lsDataName)
        {
            int lnR = 0;
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamSave", MaxFactry.Core.MaxEnumGroup.LogDebug, "save stream {Container} for {Key}", this.Container, lsDataName));
            MaxIdGuidDataModel loDataModel = loData.DataModel as MaxIdGuidDataModel;
            if (null != loDataModel)
            {
                object loValueType = loData.DataModel.GetValueType(lsDataName);
                if (loData.GetIsChanged(lsDataName))
                {
                    try
                    {
                        //// Check defined storage type
                        if (typeof(MaxLongString).Equals(loValueType) || typeof(byte[]).Equals(loValueType) || typeof(Stream).Equals(loValueType))
                        {
                            object loValue = loData.Get(lsDataName);
                            if (null != loValue && (loValue is Stream || loValue is string || loValue is byte[]))
                            {
                                string lsContentType = "application/octet-stream";
                                Stream loStream = null;
                                if (loValue is string && ((string)loValue).StartsWith(MaxAzureTableLibrary.AzureStringStreamIndicator))
                                {
                                    lsContentType = "text/plain";
                                    string lsValue = ((string)loValue).Substring(MaxAzureTableLibrary.AzureStringStreamIndicator.Length);
                                    loData.Set(lsDataName, lsValue);
                                    loStream = new MemoryStream(System.Text.UTF8Encoding.UTF8.GetBytes(lsValue));
                                }
                                else if (loValue is string && ((string)loValue).Length > this.MaxStringLength)
                                {
                                    //// Store as stream 
                                    lsContentType = "text/plain";
                                    loData.Set(lsDataName, MaxDataModel.StreamStringIndicator);
                                    loStream = new MemoryStream(System.Text.UTF8Encoding.UTF8.GetBytes(((string)loValue)));
                                }
                                else if (loValue is byte[] && ((byte[])loValue).Length > this.MaxByteLength)
                                {
                                    //// Store as stream 
                                    loData.Set(lsDataName, MaxDataModel.StreamByteIndicator);
                                    loStream = new MemoryStream((byte[])loValue);
                                }
                                else if (loValue is Stream)
                                {
                                    loStream = (Stream)loValue;
                                }

                                if (null != loStream)
                                {
                                    string lsContentTypeKey = lsDataName + "Type";
                                    object loContentType = loData.Get(lsContentTypeKey);
                                    if (null != loContentType)
                                    {
                                        lsContentType = MaxConvertLibrary.ConvertToString(typeof(object), loContentType);
                                    }

                                    string[] laStreamPath = loData.GetStreamPath();
                                    string lsStreamPath = laStreamPath[0];
                                    for (int lnP = 1; lnP < laStreamPath.Length; lnP++)
                                    {
                                        lsStreamPath += "/" + laStreamPath[lnP];
                                    }

                                    lsStreamPath += "/" + lsDataName;
                                    int lnTry = 0;
                                    MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamSave", MaxFactry.Core.MaxEnumGroup.LogDebug, "saving stream {StreamPath}", lsStreamPath));
                                    bool lbIsSuccess = false;
                                    while (!lbIsSuccess && lnTry < 3)
                                    {
                                        lbIsSuccess = MaxAzureBlobLibrary.StreamSave(
                                            this.AccountName,
                                            this.AccountKey,
                                            this.Container.ToLowerInvariant(),
                                            lsStreamPath,
                                            loStream,
                                            lsContentType,
                                            true);
                                        lnTry++;
                                        System.Threading.Thread.Sleep(100);
                                    }

                                    if (lnTry >= 3)
                                    {
                                        MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamSave", MaxFactry.Core.MaxEnumGroup.LogError, "saving stream {StreamPath} failed after {Try}", lsStreamPath, lnTry));
                                    }
                                    else if (lnTry > 1)
                                    {
                                        MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamSave", MaxFactry.Core.MaxEnumGroup.LogWarning, "saving stream {StreamPath} took multiple tries {Try}", lsStreamPath, lnTry));
                                    }
                                }
                            }
                        }
                        else
                        {
                            lnR = lnR + 4; //// Defined type of value is not a stream type
                        }
                    }
                    catch (Exception loE)
                    {
                        MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamSave", MaxEnumGroup.LogError, "Exception saving stream for {DataName}", loE, lsDataName));
                        lnR += 1; //// Exception saving stream
                    }
                }
                else if (typeof(MaxLongString).Equals(loValueType) || typeof(byte[]).Equals(loValueType) || typeof(Stream).Equals(loValueType))
                {
                    lnR = lnR + 2; //// Data is not changed
                    object loValue = loData.Get(lsDataName);
                    if (null != loValue && (loValue is Stream || loValue is string || loValue is byte[]))
                    {
                        if (loValue is string && ((string)loValue).StartsWith(MaxAzureTableLibrary.AzureStringStreamIndicator))
                        {
                            string lsValue = ((string)loValue).Substring(MaxAzureTableLibrary.AzureStringStreamIndicator.Length);
                            loData.Set(lsDataName, lsValue);
                        }
                        else if (loValue is string && ((string)loValue).Length > this.MaxByteLength)
                        {
                            //// Store as stream if over 16K in length
                            loData.Set(lsDataName, MaxDataModel.StreamStringIndicator);
                        }
                        else if (loValue is byte[] && ((byte[])loValue).Length > this.MaxByteLength)
                        {
                            //// Store as stream if over 16K in length
                            loData.Set(lsDataName, MaxDataModel.StreamByteIndicator);
                        }
                    }
                }
            }

            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamSave", MaxFactry.Core.MaxEnumGroup.LogWarning, "save stream {Container} for {Key} in {Milliseconds}", this.Container, lsDataName, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamSave", MaxFactry.Core.MaxEnumGroup.LogDebug, "save stream {Container} for {Key} in {Milliseconds}", this.Container, lsDataName, loWatch.Elapsed.TotalMilliseconds));
            }

            return lnR;
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data element</param>
        /// <param name="lsDataName">Data element name to open</param>
        /// <returns>Stream that was opened.</returns>
        public override Stream StreamOpen(MaxData loData, string lsDataName)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            Stream loR = null;
            string[] laStreamPath = loData.GetStreamPath();
            string lsStreamPath = laStreamPath[0];
            for (int lnP = 1; lnP < laStreamPath.Length; lnP++)
            {
                lsStreamPath += "/" + laStreamPath[lnP];
            }

            lsStreamPath += "/" + lsDataName;
            loR = MaxAzureBlobLibrary.StreamOpen(
                        this.AccountName,
                        this.AccountKey,
                        this.Container.ToLowerInvariant(),
                        lsStreamPath);

            //// Check for previous convention for file name
            if (null == loR)
            {
                if (loData.DataModel is MaxIdGuidDataModel)
                {
                    MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                    string lsStreamPathPrevious = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsDataName);
                    loR = MaxAzureBlobLibrary.StreamOpen(
                            this.AccountName,
                            this.AccountKey,
                            this.Container.ToLowerInvariant(),
                            lsStreamPathPrevious);
                    if (null != loR)
                    {
                        //// copy the stream to the new convention
                        if (MaxAzureBlobLibrary.StreamCopy(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPathPrevious, this.Container.ToLowerInvariant(), lsStreamPath))
                        {
                            //// Delete it from the previous convention
                            MaxAzureBlobLibrary.StreamDelete(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPathPrevious);
                        }
                    }
                }
            }


            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamOpen", MaxFactry.Core.MaxEnumGroup.LogWarning, "open stream {Container} for {Key} in {Milliseconds}", this.Container, lsDataName, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamOpen", MaxFactry.Core.MaxEnumGroup.LogDebug, "open stream {Container} for {Key} in {Milliseconds}", this.Container, lsDataName, loWatch.Elapsed.TotalMilliseconds));
            }

            return loR;
        }

        /// <summary>
        /// Deletes a stream data in storage
        /// </summary>
        /// <param name="loData">The data element</param>
        /// <param name="lsDataName">Data element name to delete</param>
        /// <returns>Flag based status code indicating level of success.</returns>
        public override int StreamDelete(MaxData loData, string lsDataName)
        {
            int lnR = 0;
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                string[] laStreamPath = loData.GetStreamPath();
                string lsStreamPath = laStreamPath[0];
                for (int lnP = 1; lnP < laStreamPath.Length; lnP++)
                {
                    lsStreamPath += "/" + laStreamPath[lnP];
                }

                lsStreamPath += "/" + lsDataName;

                if (!MaxAzureBlobLibrary.StreamDelete(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPath))
                {
                    lnR |= 2;
                }

                if (loData.DataModel is MaxIdGuidDataModel)
                {
                    //// Delete previous convention
                    MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                    string lsStreamPathPrevious = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsDataName);
                    if (!MaxAzureBlobLibrary.StreamDelete(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPathPrevious))
                    {
                        lnR |= 2;
                    }
                }

                loWatch.Stop();
                if (loWatch.Elapsed.TotalMilliseconds > 1000)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamDelete", MaxFactry.Core.MaxEnumGroup.LogWarning, "delete stream {Container} for {Key} in {Milliseconds}", this.Container, lsDataName, loWatch.Elapsed.TotalMilliseconds));
                }
                else
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamDelete", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete stream {Container} for {Key} in {Milliseconds}", this.Container, lsDataName, loWatch.Elapsed.TotalMilliseconds));
                }
            }
            catch (Exception loE)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "StreamDelete", MaxFactry.Core.MaxEnumGroup.LogError, "delete stream {Container} for {Key} failed with {Exception}", this.Container, lsDataName, loE));
                lnR |= 1;
            }

            return lnR;
        }

        /// <summary>
        /// Gets the Url to use to access the stream.
        /// </summary>
        /// <param name="loData">Data used to help determine url.</param>
        /// <param name="lsDataName">Name of data element to get the url for</param>
        /// <returns>Url to access the stream.</returns>
        public override string GetStreamUrl(MaxData loData, string lsDataName)
        {
            string lsR = string.Empty;
            string lsKeyName = lsDataName + "Name";
            string lsName = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(lsKeyName));
            if (!string.IsNullOrEmpty(lsName))
            {
                string[] laStreamPath = loData.GetStreamPath();
                string lsStreamPath = laStreamPath[0];
                for (int lnP = 1; lnP < laStreamPath.Length; lnP++)
                {
                    lsStreamPath += "/" + laStreamPath[lnP];
                }

                string lsStreamUrl = lsStreamPath + "/" + lsName;
                lsStreamPath += "/" + lsDataName;

                string lsBaseUrl = string.Format("{0}.blob.core.windows.net", this.AccountName);
                if (!string.IsNullOrEmpty(this.Cdn))
                {
                    lsBaseUrl = string.Format("{0}", this.Cdn);
                }

                lsR = string.Format("https://{0}/{1}/{2}", lsBaseUrl, this.Container.ToLowerInvariant() + "-public", lsStreamUrl);
                if (!MaxAzureBlobLibrary.StreamExists(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant() + "-public", lsStreamUrl))
                {
                    lsR = string.Empty;
                    if (!MaxAzureBlobLibrary.StreamExists(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPath))
                    {
                        if (loData.DataModel is MaxIdGuidDataModel)
                        {
                            MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                            string lsStreamPathPrevious = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsDataName);
                            if (MaxAzureBlobLibrary.StreamExists(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPathPrevious))
                            {
                                //// copy the stream to the new convention
                                if (MaxAzureBlobLibrary.StreamCopy(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPathPrevious, this.Container.ToLowerInvariant(), lsStreamPath))
                                {
                                    //// Delete it from the previous convention
                                    MaxAzureBlobLibrary.StreamDelete(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPathPrevious);
                                }
                            }
                        }
                    }

                    if (MaxAzureBlobLibrary.StreamCopy(this.AccountName, this.AccountKey, this.Container.ToLowerInvariant(), lsStreamPath, this.Container.ToLowerInvariant() + "-public", lsStreamUrl))
                    {
                        lsR = string.Format("https://{0}/{1}/{2}", lsBaseUrl, this.Container.ToLowerInvariant() + "-public", lsStreamUrl);
                    }
                }
            }

            return lsR;
        }
    }
}