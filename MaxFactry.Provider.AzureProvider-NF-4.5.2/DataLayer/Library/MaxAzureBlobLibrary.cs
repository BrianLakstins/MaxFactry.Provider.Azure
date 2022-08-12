// <copyright file="MaxAzureBlobLibrary.cs" company="Lakstins Family, LLC">
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
// <change date="6/3/2021" author="Brian A. Lakstins" description="Initial creation based on AzureTableLibrary">
// <change date="10/11/2021" author="Brian A. Lakstins" description="Reduce errors being logged based on existance checking">
// </changelog>
#endregion Change Log

namespace MaxFactry.Provider.AzureProvider.DataLayer
{
    using System;
    using System.IO;
    using MaxFactry.Core;
    using MaxFactry.Base.DataLayer;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Provides session services using MaxFactryLibrary.
    /// </summary>
    public class MaxAzureBlobLibrary
    {
        /// <summary>
        /// Gets the name of the file
        /// </summary>
        /// <param name="loData">Used to access the DataModel</param>
        /// <param name="lsBase">Type of the DataModel</param>
        /// <param name="lsKey">Field in the DataModel</param>
        /// <returns>Name of file stored in AzureStorage BLOB</returns>
        public static string GetStreamFileName(MaxData loData, string lsBase, string lsKey)
        {
            string lsStorageKey = MaxConvertLibrary.ConvertToString(typeof(object), loData.Get(loData.DataModel.StorageKey));
            if (lsKey.Contains("."))
            {
                return lsStorageKey + "/" + loData.DataModel.DataStorageName + "/" + lsBase + "/" + lsKey;
            }

            return lsStorageKey + "/" + loData.DataModel.DataStorageName + "/" + lsBase + "_" + lsKey;
        }

        /// <summary>
        /// Writes stream data to storage.
        /// </summary>
        /// <param name="lsAccountName">Azure Storage Account Name</param>
        /// <param name="lsAccountKey">Azure Storage Account Key</param>
        /// <param name="lsStorageLocation">Location used to store the blob</param>
        /// <param name="lsFileName">Name of the file to save in the storage location.</param>
        /// <param name="loStream">Data element name to write</param>
        /// <param name="lsContentType">Mime type of the content.</param>
        /// <returns>True of successful</returns>
        public static bool StreamSave(string lsAccountName, string lsAccountKey, string lsStorageLocation, string lsFileName, Stream loStream, string lsContentType, bool lbUpdateIfExists)
        {
            bool lbR = false;

            try
            {
                CloudStorageAccount loAccount = null;
                if (lsAccountName == "UseDevelopmentStorage")
                {
                    loAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
                }
                else
                {
                    loAccount = new CloudStorageAccount(new StorageCredentials(lsAccountName, lsAccountKey), true);
                }

                // Create the blob client.
                CloudBlobClient loBlobClient = loAccount.CreateCloudBlobClient();

                // Retrieve a reference to a container.
                CloudBlobContainer loContainer = loBlobClient.GetContainerReference(lsStorageLocation);

                if (!loContainer.Exists())
                {
                    // Create the container if it doesn't already exist.
                    if (loContainer.CreateIfNotExists())
                    {
                        if (lsStorageLocation.EndsWith("-public"))
                        {
                            loContainer.SetPermissions(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });
                        }
                        else
                        {
                            loContainer.SetPermissions(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Off });
                        }
                    }
                }

                // Retrieve reference to the blob based on the key.
                CloudBlockBlob loBlockBlob = loContainer.GetBlockBlobReference(lsFileName);
                if (!string.IsNullOrEmpty(lsContentType))
                {
                    loBlockBlob.Properties.ContentType = lsContentType;
                }

                // Cache for up to 1 year.
                loBlockBlob.Properties.CacheControl = "no-transform,public,max-age=31536000,s-maxage=31536000";
                if (lbUpdateIfExists || !loBlockBlob.Exists())
                {
                    loBlockBlob.UploadFromStream(loStream);
                    lbR = true;
                }
            }
            catch (Exception loE)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureTableStreamSave", MaxEnumGroup.LogError, "Error Saving stream to {AccountName} for {StorageLocation} in {File}", loE, lsAccountName, lsStorageLocation, lsFileName));
            }

            return lbR;
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public static Stream StreamOpen(string lsAccountName, string lsAccountKey, string lsStorageLocation, string lsFileName)
        {
            // Create the blob client.
            CloudBlobClient loBlobClient = new CloudStorageAccount(new StorageCredentials(lsAccountName, lsAccountKey), true).CreateCloudBlobClient();

            // Retrieve a reference to a container.
            CloudBlobContainer loContainer = loBlobClient.GetContainerReference(lsStorageLocation.ToLower());

            if (loContainer.Exists())
            {
                CloudBlockBlob loBlockBlob = loContainer.GetBlockBlobReference(lsFileName);
                if (loBlockBlob.Exists())
                {
                    MemoryStream loStream = new MemoryStream();
                    loBlockBlob.DownloadToStream(loStream);
                    loStream.Position = 0;
                    return loStream;
                }
            }

            return null;
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public static bool StreamCopy(string lsAccountName, string lsAccountKey, string lsStorageLocation, string lsFileName, string lsStorageLocationNew, string lsFileNameNew)
        {
            bool lbR = false;
            // Create the blob client.
            CloudBlobClient loBlobClient = new CloudStorageAccount(new StorageCredentials(lsAccountName, lsAccountKey), true).CreateCloudBlobClient();

            // Retrieve a reference to a container.
            CloudBlobContainer loContainer = loBlobClient.GetContainerReference(lsStorageLocation.ToLower());

            if (loContainer.Exists())
            {
                CloudBlockBlob loBlockBlob = loContainer.GetBlockBlobReference(lsFileName);
                if (loBlockBlob.Exists())
                {
                    CloudBlobContainer loContainerNew = loBlobClient.GetContainerReference(lsStorageLocationNew.ToLower());
                    if (loContainerNew.CreateIfNotExists())
                    {
                        if (lsStorageLocationNew.EndsWith("-public"))
                        {
                            loContainerNew.SetPermissions(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });
                        }
                        else
                        {
                            loContainerNew.SetPermissions(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Off });
                        }
                    }

                    CloudBlockBlob loBlockBlobNew = loContainerNew.GetBlockBlobReference(lsFileNameNew);
                    loBlockBlobNew.Properties.ContentType = loBlockBlob.Properties.ContentType;

                    // Cache for up to 1 year.
                    loBlockBlobNew.Properties.CacheControl = "no-transform,public,max-age=31536000,s-maxage=31536000";
                    MemoryStream loStream = new MemoryStream();
                    try
                    {
                        loBlockBlob.DownloadToStream(loStream);
                        loStream.Position = 0;
                        loBlockBlobNew.UploadFromStream(loStream);
                        lbR = true;
                    }
                    finally
                    {
                        loStream.Dispose();
                    }
                }
            }

            return lbR;
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public static bool StreamExists(string lsAccountName, string lsAccountKey, string lsStorageLocation, string lsFileName)
        {
            bool lbR = false;
            // Create the blob client.
            CloudBlobClient loBlobClient = new CloudStorageAccount(new StorageCredentials(lsAccountName, lsAccountKey), true).CreateCloudBlobClient();

            // Retrieve a reference to a container.
            CloudBlobContainer loContainer = loBlobClient.GetContainerReference(lsStorageLocation.ToLower());

            if (loContainer.Exists())
            {
                CloudBlockBlob loBlockBlob = loContainer.GetBlockBlobReference(lsFileName);
                if (loBlockBlob.Exists())
                {
                    lbR = true;
                }
            }

            return lbR;
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public static bool StreamDelete(string lsAccountName, string lsAccountKey, string lsStorageLocation, string lsFileName)
        {
            // Create the blob client.
            CloudBlobClient loBlobClient = new CloudStorageAccount(new StorageCredentials(lsAccountName, lsAccountKey), true).CreateCloudBlobClient();

            // Retrieve a reference to a container.
            CloudBlobContainer loContainer = loBlobClient.GetContainerReference(lsStorageLocation.ToLower());

            if (loContainer.Exists())
            {
                // Retrieve reference to the blob based on the key.
                CloudBlockBlob loBlockBlob = loContainer.GetBlockBlobReference(lsFileName);
                if (loBlockBlob.Exists())
                {
                    loBlockBlob.Delete();
                    return true;
                }
            }

            return false;
        }
    }
}