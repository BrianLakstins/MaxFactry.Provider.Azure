// <copyright file="MaxDataContextAzureSqlProvider.cs" company="Lakstins Family, LLC">
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
// <change date="5/28/2021" author="Brian A. Lakstins" description="Initial creation">
// </changelog>
#endregion

namespace MaxFactry.Base.DataLayer.Provider
{
    using System;
    using System.IO;
    using MaxFactry.Base.DataLayer;
    using MaxFactry.Base.DataLayer.Library.Provider;
    using MaxFactry.Core;
    using MaxFactry.Provider.AzureProvider.DataLayer;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;

    /// <summary>
    /// Data Context used to work with database data on SQL (Could be Azure SQL or local SQL) and Stream storage on Azure
    /// </summary>
    public class MaxDataContextAzureSqlProvider : MaxDataContextMSSqlProvider
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
            return MaxDataStreamAzureBlobLibrary.StreamSave(loData, lsKey, this.Container, this.AccountName, this.AccountKey, 2000, 2000);
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public override Stream StreamOpen(MaxData loData, string lsKey)
        {
            return MaxDataStreamAzureBlobLibrary.StreamOpen(loData, lsKey, this.Container, this.AccountName, this.AccountKey);
        }

        /// <summary>
        /// Deletes a stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public override bool StreamDelete(MaxData loData, string lsKey)
        {
            return MaxDataStreamAzureBlobLibrary.StreamDelete(loData, lsKey, this.Container, this.AccountName, this.AccountKey);
        }

        /// <summary>
        /// Gets the Url to use to access the stream.
        /// </summary>
        /// <param name="loData">Data used to help determine url.</param>
        /// <param name="lsKey">Key used to help determine key.</param>
        /// <returns>Url to access the stream.</returns>
        public override string GetStreamUrl(MaxData loData, string lsKey)
        {
            return MaxDataStreamAzureBlobLibrary.GetStreamUrl(loData, lsKey, this.Container, this.AccountName, this.AccountKey, this.Cdn);
        }
    }
}