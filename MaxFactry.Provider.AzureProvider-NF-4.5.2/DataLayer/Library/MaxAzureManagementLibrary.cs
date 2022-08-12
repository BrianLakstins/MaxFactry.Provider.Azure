// <copyright file="MaxAzureManagementLibrary.cs" company="Lakstins Family, LLC">
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
// <change date="5/17/2015" author="Brian A. Lakstins" description="Initial creation">
// </changelog>
#endregion Change Log

namespace MaxFactry.Provider.AzureProvider.DataLayer
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading.Tasks;
    using System.Xml.Linq;
    using MaxFactry.Core;
    using MaxFactry.Base.DataLayer;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Management.WebSites;
    using Microsoft.WindowsAzure.Management.WebSites.Models;

    /// <summary>
    /// Provides session services using MaxFactryLibrary.
    /// </summary>
    public class MaxAzureManagementLibrary
    {
        /// <summary>
        /// Restarts a web site
        /// </summary>
        /// <param name="lsWebSpaceName">From Microsoft.WindowsAzure.Management.WebSites.Models.WebSpaceNames</param>
        /// <param name="lsWebSiteName">Name of the web site.</param>
        /// <param name="lsPublishFilePath">Path to Publish file to get SubscriptionId and Management Certificate</param>
        /// <returns>true if restarted.</returns>
        public static bool RestartWebSite(string lsWebSpaceName, string lsWebSiteName, string lsPublishFilePath)
        {
            FileStream loStream = File.OpenRead(lsPublishFilePath);
            XElement loSubscription = null;
            try
            {
                XDocument loDocument = XDocument.Load(loStream);
                IEnumerable<XElement> loSubscriptionList = loDocument.Descendants("Subscription");
                foreach (XElement loElement in loSubscriptionList)
                {
                    if (null == loSubscription)
                    {
                        loSubscription = loElement;
                    }
                }
            }
            finally
            {
                loStream = null;
            }

            if (null != loSubscription)
            {
                string lsSubscriptionId = loSubscription.Attribute("Id").Value;
                string lsManagementCertificate = loSubscription.Attribute("ManagementCertificate").Value;
                return RestartWebSite(lsWebSpaceName, lsWebSiteName, lsSubscriptionId, lsManagementCertificate);
            }

            return false;
        }

        /// <summary>
        /// Restarts a web site
        /// </summary>
        /// <param name="lsWebSpaceName">From Microsoft.WindowsAzure.Management.WebSites.Models.WebSpaceNames</param>
        /// <param name="lsWebSiteName">Name of the web site.</param>
        /// <param name="lsSubscriptionId">Subscription Id of Azure account</param>
        /// <param name="lsManagementCertificate">Management certificate associated with Azure Account</param>
        /// <returns>true if restarted.</returns>
        public static bool RestartWebSite(string lsWebSpaceName, string lsWebSiteName, string lsSubscriptionId, string lsManagementCertificate)
        {
            SubscriptionCloudCredentials loCredentials = new CertificateCloudCredentials(
                    lsSubscriptionId,
                    new X509Certificate2(Convert.FromBase64String(lsManagementCertificate)));

            WebSiteManagementClient loClient = CloudContext.Clients.CreateWebSiteManagementClient(loCredentials);
            OperationResponse loResponse = loClient.WebSites.Restart(lsWebSpaceName, lsWebSiteName);
            if (loResponse.StatusCode == System.Net.HttpStatusCode.OK)
            {
                return true;
            }

            return false;
        }
    }
}