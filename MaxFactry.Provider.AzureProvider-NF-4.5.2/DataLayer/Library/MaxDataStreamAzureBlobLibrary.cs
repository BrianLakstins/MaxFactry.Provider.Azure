// <copyright file="MaxDataStreamAzureBlobLibrary.cs" company="Lakstins Family, LLC">
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
// <change date="5/29/2021" author="Brian A. Lakstins" description="Initial creation">
// </changelog>
#endregion

namespace MaxFactry.Provider.AzureProvider.DataLayer
{
    using System;
    using System.IO;
    using MaxFactry.Base.DataLayer;
    using MaxFactry.Core;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class MaxDataStreamAzureBlobLibrary
    {
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
        public static bool StreamSave(MaxData loData, string lsKey, string lsContainer, string lsAccountName, string lsAccountKey, int lnMaxStringLength, int lnMaxByteLength)
        {
            bool lbR = false;
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveStart", MaxFactry.Core.MaxEnumGroup.LogDebug, "save stream {Container} for {Key}", lsContainer, lsKey));
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
                            else if (loValue is string && ((string)loValue).Length > lnMaxStringLength)
                            {
                                //// Store as stream 
                                lsContentType = "text/plain";
                                loData.Set(lsKey, MaxDataModel.StreamStringIndicator);
                                loStream = new MemoryStream(System.Text.UTF8Encoding.UTF8.GetBytes(((string)loValue)));
                            }
                            else if (loValue is byte[] && ((byte[])loValue).Length > lnMaxByteLength)
                            {
                                //// Store as stream 
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

                                string[] laStreamPath = loData.GetStreamPath();
                                string lsStreamPath = laStreamPath[0];
                                for (int lnP = 1; lnP < laStreamPath.Length; lnP++)
                                {
                                    lsStreamPath += "/" + laStreamPath[lnP];
                                }

                                lsStreamPath += "/" + lsKey;
                                int lnTry = 0;
                                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogDebug, "saving stream {StreamPath}", lsStreamPath));
                                while (!lbR && lnTry < 3)
                                {
                                    lbR = MaxAzureBlobLibrary.StreamSave(
                                        lsAccountName,
                                        lsAccountKey,
                                        lsContainer.ToLowerInvariant(),
                                        lsStreamPath,
                                        loStream,
                                        lsContentType,
                                        true);
                                    lnTry++;
                                    System.Threading.Thread.Sleep(100);
                                }

                                if (lnTry >= 3)
                                {
                                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogError, "saving stream {StreamPath} failed after {Try}", lsStreamPath, lnTry));
                                }
                                else if (lnTry > 1)
                                {
                                    MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSave", MaxFactry.Core.MaxEnumGroup.LogWarning, "saving stream {StreamPath} took multiple tries {Try}", lsStreamPath, lnTry));
                                }
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
                        else if (loValue is string && ((string)loValue).Length > lnMaxByteLength)
                        {
                            //// Store as stream if over 16K in length
                            loData.Set(lsKey, MaxDataModel.StreamStringIndicator);
                        }
                        else if (loValue is byte[] && ((byte[])loValue).Length > lnMaxByteLength)
                        {
                            //// Store as stream if over 16K in length
                            loData.Set(lsKey, MaxDataModel.StreamByteIndicator);
                        }
                    }
                }
            }

            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "save stream {Container} for {Key} in {Milliseconds}", lsContainer, lsKey, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "save stream {Container} for {Key} in {Milliseconds}", lsContainer, lsKey, loWatch.Elapsed.TotalMilliseconds));
            }

            return lbR;
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public static Stream StreamOpen(MaxData loData, string lsKey, string lsContainer, string lsAccountName, string lsAccountKey)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            Stream loR = null;
            string[] laStreamPath = loData.GetStreamPath();
            string lsStreamPath = laStreamPath[0];
            for (int lnP = 1; lnP < laStreamPath.Length; lnP++)
            {
                lsStreamPath += "/" + laStreamPath[lnP];
            }

            lsStreamPath += "/" + lsKey;
            loR = MaxAzureBlobLibrary.StreamOpen(
                        lsAccountName,
                        lsAccountKey,
                        lsContainer.ToLowerInvariant(),
                        lsStreamPath);

            //// Check for previous convention for file name
            if (null == loR)
            {
                if (loData.DataModel is MaxIdGuidDataModel)
                {
                    MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                    string lsStreamPathPrevious = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsKey);
                    loR = MaxAzureBlobLibrary.StreamOpen(
                            lsAccountName,
                            lsAccountKey,
                            lsContainer.ToLowerInvariant(),
                            lsStreamPathPrevious);
                    if (null != loR)
                    {
                        //// copy the stream to the new convention
                        if (MaxAzureBlobLibrary.StreamCopy(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPathPrevious, lsContainer.ToLowerInvariant(), lsStreamPath))
                        {
                            //// Delete it from the previous convention
                            MaxAzureBlobLibrary.StreamDelete(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPathPrevious);
                        }
                    }
                }
            }


            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "open stream {Container} for {Key} in {Milliseconds}", lsContainer, lsKey, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "open stream {Container} for {Key} in {Milliseconds}", lsContainer, lsKey, loWatch.Elapsed.TotalMilliseconds));
            }

            return loR;
        }

        /// <summary>
        /// Deletes a stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public static bool StreamDelete(MaxData loData, string lsKey, string lsContainer, string lsAccountName, string lsAccountKey)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            bool lbR = false;
            string[] laStreamPath = loData.GetStreamPath();
            string lsStreamPath = laStreamPath[0];
            for (int lnP = 1; lnP < laStreamPath.Length; lnP++)
            {
                lsStreamPath += "/" + laStreamPath[lnP];
            }

            lsStreamPath += "/" + lsKey;

            if (MaxAzureBlobLibrary.StreamDelete(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPath))
            {
                lbR = true;
            }

            if (loData.DataModel is MaxIdGuidDataModel)
            {
                //// Delete previous convention
                MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                string lsStreamPathPrevious = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsKey);
                if (MaxAzureBlobLibrary.StreamDelete(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPathPrevious))
                {
                    lbR = true;
                }
            }

            loWatch.Stop();
            if (loWatch.Elapsed.TotalMilliseconds > 1000)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveEnd", MaxFactry.Core.MaxEnumGroup.LogWarning, "delete stream {Container} for {Key} in {Milliseconds}", lsContainer, lsKey, loWatch.Elapsed.TotalMilliseconds));
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure("AzureStorageStreamSaveEnd", MaxFactry.Core.MaxEnumGroup.LogDebug, "delete stream {Container} for {Key} in {Milliseconds}", lsContainer, lsKey, loWatch.Elapsed.TotalMilliseconds));
            }

            return lbR;
        }

        /// <summary>
        /// Gets the Url to use to access the stream.
        /// </summary>
        /// <param name="loData">Data used to help determine url.</param>
        /// <param name="lsKey">Key used to help determine key.</param>
        /// <returns>Url to access the stream.</returns>
        public static string GetStreamUrl(MaxData loData, string lsKey, string lsContainer, string lsAccountName, string lsAccountKey, string lsCdn)
        {
            string lsR = string.Empty;
            string lsKeyName = lsKey + "Name";
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
                lsStreamPath += "/" + lsKey;

                string lsBaseUrl = string.Format("{0}.blob.core.windows.net", lsAccountName);
                if (!string.IsNullOrEmpty(lsCdn))
                {
                    lsBaseUrl = string.Format("{0}", lsCdn);
                }

                lsR = string.Format("https://{0}/{1}/{2}", lsBaseUrl, lsContainer.ToLowerInvariant() + "-public", lsStreamUrl);
                if (!MaxAzureBlobLibrary.StreamExists(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant() + "-public", lsStreamUrl))
                {
                    lsR = string.Empty;
                    if (!MaxAzureBlobLibrary.StreamExists(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPath))
                    {
                        if (loData.DataModel is MaxIdGuidDataModel)
                        {
                            MaxIdGuidDataModel loDataModel = (MaxIdGuidDataModel)loData.DataModel;
                            string lsStreamPathPrevious = MaxAzureBlobLibrary.GetStreamFileName(loData, loData.Get(loDataModel.Id).ToString(), lsKey);
                            if (MaxAzureBlobLibrary.StreamExists(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPathPrevious))
                            {
                                //// copy the stream to the new convention
                                if (MaxAzureBlobLibrary.StreamCopy(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPathPrevious, lsContainer.ToLowerInvariant(), lsStreamPath))
                                {
                                    //// Delete it from the previous convention
                                    MaxAzureBlobLibrary.StreamDelete(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPathPrevious);
                                }
                            }
                        }
                    }

                    if (MaxAzureBlobLibrary.StreamCopy(lsAccountName, lsAccountKey, lsContainer.ToLowerInvariant(), lsStreamPath, lsContainer.ToLowerInvariant() + "-public", lsStreamUrl))
                    {
                        lsR = string.Format("https://{0}/{1}/{2}", lsBaseUrl, lsContainer.ToLowerInvariant() + "-public", lsStreamUrl);
                    }
                }
            }

            return lsR;
        }
    }
}
