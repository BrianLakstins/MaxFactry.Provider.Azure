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
        /// To be run after providers have been registered
        /// </summary>
        /// <param name="loConfig">The configuration for the default repository provider.</param>
        public virtual void SetProviderConfiguration(MaxIndex loConfig)
        {
            loConfig.Add(typeof(MaxFactry.Base.DataLayer.Provider.MaxDataContextAzureSqlProvider).Name, typeof(MaxFactry.Base.DataLayer.Provider.MaxDataContextAzureSqlProvider));
            loConfig.Add(typeof(MaxFactry.Base.DataLayer.Provider.MaxDataContextAzureStorageProvider).Name, typeof(MaxFactry.Base.DataLayer.Provider.MaxDataContextAzureStorageProvider));
            loConfig.Add(typeof(MaxFactry.Base.DataLayer.Provider.MaxDataContextAzureTableProvider).Name, typeof(MaxFactry.Base.DataLayer.Provider.MaxDataContextAzureTableProvider));
            //// Run in the app MaxStartup with the some real keys or connection strings
            this.SetProviderConfigurationInstrumentationKey(loConfig, string.Empty, string.Empty);
            this.SetProviderConfigurationConnectionString(loConfig, string.Empty, string.Empty);
        }

        public virtual void SetProviderConfigurationInstrumentationKey(MaxIndex loConfig, string lsProduction, string lsDev)
        {
            loConfig.Add(MaxLogLibraryAzureApplicationInsightProvider.InstrumentationKeyConfigName, lsProduction);
#if DEBUG
            loConfig.Add(MaxLogLibraryAzureApplicationInsightProvider.InstrumentationKeyConfigName, lsDev);
#else
            if (System.Diagnostics.Debugger.IsAttached)
            {
                loConfig.Add(MaxLogLibraryAzureApplicationInsightProvider.InstrumentationKeyConfigName, lsDev);
            }
#endif
        }

        public virtual void SetProviderConfigurationConnectionString(MaxIndex loConfig, string lsProduction, string lsDev)
        {
            loConfig.Add(MaxLogLibraryAzureApplicationInsightProvider.ConnectionStringConfigName, lsProduction);
#if DEBUG
            loConfig.Add(MaxLogLibraryAzureApplicationInsightProvider.ConnectionStringConfigName, lsDev);
#else
            if (System.Diagnostics.Debugger.IsAttached)
            {
                loConfig.Add(MaxLogLibraryAzureApplicationInsightProvider.ConnectionStringConfigName, lsDev);
            }
#endif
        }

        /// <summary>
        /// To be run first, before anything else in the application.
        /// </summary>
        public virtual void RegisterProviders()
        {
            this.RegisterProviderAzureApplicationInsightProvider();
        }

        public virtual void RegisterProviderAzureApplicationInsightProvider()
        {
            MaxFactry.Core.MaxLogLibrary.Instance.ProviderAdd(typeof(MaxFactry.Core.Provider.MaxLogLibraryAzureApplicationInsightProvider));
        }

        /// <summary>
        /// To be run after providers have been configured.
        /// </summary>
        public virtual void ApplicationStartup()
        {
        }
    }
}
