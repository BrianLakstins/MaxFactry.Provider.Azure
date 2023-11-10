// <copyright file="MaxStartup.cs" company="Lakstins Family, LLC">
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
// <change date="7/24/2023" author="Brian A. Lakstins" description="Initial creation">
// </changelog>
#endregion

namespace MaxFactry.Provider.AzureProvider
{
    using System;
    using MaxFactry.Base.DataLayer.Provider;
    using MaxFactry.Core;
    using MaxFactry.Core.Provider;

    /// <summary>
    /// Class used to define initialization tasks for a module or provider.
    /// </summary>
    public class MaxStartup
    {
        /// <summary>
        /// Internal storage of single object
        /// </summary>
        private static object _oInstance = null;

        /// <summary>
        /// Lock object for multi-threaded access.
        /// </summary>
        private static object _oLock = new object();

        protected static object CreateInstance(System.Type loType, object loCurrent)
        {
            if (null == loCurrent)
            {
                lock (_oLock)
                {
                    if (null == loCurrent)
                    {
                        loCurrent = MaxFactry.Core.MaxFactryLibrary.CreateSingleton(loType);
                    }
                }
            }

            return loCurrent;
        }

        /// <summary>
        /// Gets the single instance of this class.
        /// </summary>
        public static MaxStartup Instance
        {
            get
            {
                _oInstance = CreateInstance(typeof(MaxStartup), _oInstance);
                return _oInstance as MaxStartup;
            }
        }

        /// <summary>
        /// Sets the conifiguration for providers
        /// These methods can be used individually
        /// this.SetProviderConfigurationInstrumentationKey(loConfig, string.Empty, string.Empty);
        /// this.SetProviderConfigurationConnectionString(loConfig, string.Empty, string.Empty);
        /// this.SetProviderConfigurationPerformanceCounterList(loConfig, "Default", "Default");
        /// </summary>
        /// <param name="loConfig">The configuration for the default repository provider.</param>
        public virtual void SetProviderConfiguration(MaxIndex loConfig)
        {
            loConfig.Add(typeof(MaxDataContextStreamAzureBlobProvider).Name, typeof(MaxDataContextStreamAzureBlobProvider));
            loConfig.Add(typeof(MaxDataContextAzureSqlProvider).Name, typeof(MaxDataContextAzureSqlProvider));
            loConfig.Add(typeof(MaxDataContextAzureTableProvider).Name, typeof(MaxDataContextAzureTableProvider));
            //// Use in the app MaxStartup with the some real keys or connection strings
            //this.SetProviderConfigurationInstrumentationKey(loConfig, string.Empty, string.Empty);
            //this.SetProviderConfigurationConnectionString(loConfig, string.Empty, string.Empty);
            //this.SetProviderConfigurationPerformanceCounterList(loConfig, "Default", "Default");
        }

        public virtual void SetProviderConfigurationInstrumentationKey(MaxIndex loConfig, string lsProduction, string lsDev)
        {
            if (MaxFactry.Core.MaxFactryLibrary.Environment == MaxEnumGroup.EnvironmentProduction)
            {
                loConfig.Add(typeof(MaxLogLibraryAzureApplicationInsightProvider) + "-" + MaxLogLibraryAzureApplicationInsightProvider.InstrumentationKeyConfigName, lsProduction);
            }
            else
            {
                loConfig.Add(typeof(MaxLogLibraryAzureApplicationInsightProvider) + "-" + MaxLogLibraryAzureApplicationInsightProvider.InstrumentationKeyConfigName, lsDev);
            }
        }

        public virtual void SetProviderConfigurationConnectionString(MaxIndex loConfig, string lsProduction, string lsDev)
        {
            if (MaxFactry.Core.MaxFactryLibrary.Environment == MaxEnumGroup.EnvironmentProduction)
            {
                loConfig.Add(typeof(MaxLogLibraryAzureApplicationInsightProvider) + "-" + MaxLogLibraryAzureApplicationInsightProvider.ConnectionStringConfigName, lsProduction);
            }
            else
            {
                loConfig.Add(typeof(MaxLogLibraryAzureApplicationInsightProvider) + "-" + MaxLogLibraryAzureApplicationInsightProvider.ConnectionStringConfigName, lsDev);
            }
        }

        public virtual void SetProviderConfigurationPerformanceCounterList(MaxIndex loConfig, string lsProduction, string lsDev)
        {
            if (MaxFactry.Core.MaxFactryLibrary.Environment == MaxEnumGroup.EnvironmentProduction)
            {
                loConfig.Add(typeof(MaxLogLibraryAzureApplicationInsightProvider) + "-" + MaxLogLibraryAzureApplicationInsightProvider.PerformanceCounterListConfigName, lsProduction);
            }
            else
            {
                loConfig.Add(typeof(MaxLogLibraryAzureApplicationInsightProvider) + "-" + MaxLogLibraryAzureApplicationInsightProvider.PerformanceCounterListConfigName, lsDev);
            }
        }

        /// <summary>
        /// Registers providers after their configuration has been set
        /// These methods can be used individually.  They are included by default when using this method.
        /// this.RegisterProviderAzureApplicationInsightProvider();
        /// this.RegisterProviderAzureSecurityTableProvider();
        /// this.RegisterProviderAzureBlobProvider();
        /// </summary>
        public virtual void RegisterProviders()
        {
            this.RegisterProviderAzureApplicationInsightProvider();
            this.RegisterProviderAzureSecurityTableProvider();
            this.RegisterProviderAzureBlobProvider();
        }

        public virtual void RegisterProviderAzureBlobProvider()
        {
            MaxFactry.Base.DataLayer.MaxDataContextStreamLibrary.Instance.ProviderSet(typeof(MaxDataContextStreamAzureBlobProvider));
        }

        public virtual void RegisterProviderAzureApplicationInsightProvider()
        {
            MaxFactry.Core.MaxLogLibrary.Instance.ProviderAdd(typeof(MaxFactry.Core.Provider.MaxLogLibraryAzureApplicationInsightProvider));
        }

        /// <summary>
        /// Only register this provider if the repository provider is AzureTable
        /// </summary>
        public virtual void RegisterProviderAzureSecurityTableProvider()
        {
            //// Azure storage provider configuration for security module
            MaxFactry.General.DataLayer.MaxSecurityRepository.Instance.ProviderAdd(
                typeof(MaxFactry.General.DataLayer.MaxUserDataModel).ToString(),
                typeof(MaxFactry.General.DataLayer.Provider.MaxSecurityRepositoryAzureTableProvider));
        }

        /// <summary>
        /// Starts the application once all providers are configured and registered.
        /// </summary>
        public virtual void ApplicationStartup()
        {
        }
    }
}
