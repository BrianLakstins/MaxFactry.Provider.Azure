// <copyright file="MaxLogLibraryAzureApplicationInsightProvider.cs" company="Lakstins Family, LLC">
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
// <change date="5/21/2019" author="Brian A. Lakstins" description="Initial creation">
// <change date="5/23/2019" author="Brian A. Lakstins" description="Integrated performance logging">
// <change date="5/23/2019" author="Brian A. Lakstins" description="Made some properties and methods virtual so they can be overridden.">
// <change date="5/29/2019" author="Brian A. Lakstins" description="Force clearing of default counters.">
// <change date="8/5/2019" author="Brian A. Lakstins" description="Fix using config.">
// <change date="4/28/2020" author="Brian A. Lakstins" description="Update to be able to report separatly on multiple apps inside the application">
// <change date="4/28/2020" author="Brian A. Lakstins" description="Fix checking for existing value">
// <change date="5/19/2020" author="Brian A. Lakstins" description="Fix for null values in log parameters">
// <change date="6/5/2020" author="Brian A. Lakstins" description="Updated for change to base.">
// <change date="7/7/2021" author="Brian A. Lakstins" description="Ignore static and debug logging because there will probably be too much.">
// </changelog>
#endregion

namespace MaxFactry.Core.Provider
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Reflection;
    using System.Threading;
    using MaxFactry.Core;
    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.ApplicationInsights.Extensibility.PerfCounterCollector;


    /// <summary>
    /// Azure Application Insights provider for the MaxFactory class to manage logging.
    /// </summary>
	public class MaxLogLibraryAzureApplicationInsightProvider : MaxLogLibraryBaseProvider, IMaxLogLibraryProvider
    {
        private static object _oLock = new object();

        private Dictionary<string, TelemetryConfiguration> _oTelemetryConfigIndex = new Dictionary<string, TelemetryConfiguration>();

        private Dictionary<string, TelemetryClient> _oTelemetryClientIndex = new Dictionary<string, TelemetryClient>();

        private string _sInstrumentationKey = null;

        private PerformanceCollectorModule _oPerformanceModule;

        /// <summary>
        /// Initializes the provider.
        /// </summary>
        /// <param name="lsName">Name of the provider.</param>
        /// <param name="loConfig">Configuration information.</param>
        public override void Initialize(string lsName, MaxIndex loConfig)
        {
            string lsKey = this.GetConfigValue(loConfig, "InstrumentationKey") as string;
            if (null != lsKey)
            {
                this._sInstrumentationKey = lsKey;
            }
            else if (null != TelemetryConfiguration.Active)
            {
                this._sInstrumentationKey = TelemetryConfiguration.Active.InstrumentationKey;
            }
        }

        protected string InstrumentationKey
        {
            get
            {
                string lsInstrumentationKey = MaxConfigurationLibrary.GetValue(MaxEnumGroup.Scope24, "InstrumentationKey") as string;
                if (null != lsInstrumentationKey && lsInstrumentationKey.Length > 0)
                {
                    return lsInstrumentationKey;
                }

                return this._sInstrumentationKey;
            }
        }

        protected virtual TelemetryConfiguration TelemetryConfig
        {
            get
            {
                string lsKey = this.InstrumentationKey;
                if (!this._oTelemetryConfigIndex.ContainsKey(lsKey))
                {
                    lock (_oLock)
                    {
                        if (!this._oTelemetryConfigIndex.ContainsKey(lsKey))
                        {
                            TelemetryConfiguration loConfig = TelemetryConfiguration.Active;
                            if (null != loConfig)
                            {
                                loConfig.InstrumentationKey = lsKey;
                                loConfig.TelemetryChannel.DeveloperMode = Debugger.IsAttached;
#if DEBUG
                                loConfig.TelemetryChannel.DeveloperMode = true;
#endif
                            }

                            this._oTelemetryConfigIndex.Add(lsKey, loConfig);
                        }
                    }
                }

                return this._oTelemetryConfigIndex[lsKey];
            }
        }

        protected virtual TelemetryClient TelemetryClient
        {
            get
            {
                string lsKey = this.InstrumentationKey;
                if (!this._oTelemetryClientIndex.ContainsKey(lsKey))
                {
                    lock (_oLock)
                    {
                        if (!this._oTelemetryClientIndex.ContainsKey(lsKey))
                        {
                            TelemetryClient loClient = null;
                            if (null != this.TelemetryConfig)
                            {
                                //// TODO: update this to use config information so that a provider can be used for the tracking information
                                loClient = new TelemetryClient(TelemetryConfig);
                                loClient.Context.Session.Id = Guid.NewGuid().ToString();
                                loClient.Context.User.Id = (Environment.UserName + Environment.MachineName).GetHashCode().ToString();
                                loClient.Context.User.AuthenticatedUserId = Environment.MachineName + "/" + Environment.UserName;
                                loClient.Context.Device.OperatingSystem = Environment.OSVersion.ToString();
                                Assembly loEntryAssembly = Assembly.GetEntryAssembly();
                                if (null != loEntryAssembly)
                                {
                                    loClient.Context.Component.Version = loEntryAssembly.GetName().Version.ToString();
                                }
                            }

                            this._oTelemetryClientIndex.Add(lsKey, loClient);
                        }
                    }
                }

                return this._oTelemetryClientIndex[lsKey]; ;
            }
        }

        public override void Log(MaxLogEntryStructure loLogEntry)
        {
            TelemetryClient loClient = this.TelemetryClient;
            if (null != loClient)
            {
                Exception loExceptionToLog = null;
                Dictionary<string, string> loPropertyIndex = new Dictionary<string, string>();
                Dictionary<string, double> loMetricIndex = new Dictionary<string, double>();
                if (null != loLogEntry.Params)
                {
                    for (int lnP = 0; lnP < loLogEntry.Params.Length; lnP++)
                    {
                        if (loLogEntry.Params[lnP] is Exception)
                        {
                            loExceptionToLog = loLogEntry.Params[lnP] as Exception;
                        }
                        else if (loLogEntry.Params[lnP] is MaxIndex)
                        {
                            MaxIndex loMaxIndex = loLogEntry.Params[lnP] as MaxIndex;
                            string[] laKey = loMaxIndex.GetSortedKeyList();
                            foreach (string lsKey in laKey)
                            {
                                loPropertyIndex.Add(lsKey, MaxConvertLibrary.ConvertToString(typeof(object), loMaxIndex[lsKey]));
                            }
                        }
                        else
                        {
                            loPropertyIndex.Add(lnP.ToString(), MaxConvertLibrary.ConvertToString(typeof(object), loLogEntry.Params[lnP]));
                        }
                    }
                }

                if (null != loExceptionToLog)
                {
                    loPropertyIndex.Add("MessageTemplate", loLogEntry.MessageTemplate);
                    loPropertyIndex.Add("Timestamp", loLogEntry.Timestamp.ToString());
                    loPropertyIndex.Add("Level", loLogEntry.Level.ToString());
                    loClient.TrackException(loExceptionToLog, loPropertyIndex, loMetricIndex);
                    loClient.Flush();
                }
                else if (MaxEnumGroup.LogDebug < loLogEntry.Level && loLogEntry.Level < MaxEnumGroup.LogStatic)
                {
                    loPropertyIndex.Add("Timestamp", loLogEntry.Timestamp.ToString());
                    loPropertyIndex.Add("Level", loLogEntry.Level.ToString());
                    loClient.TrackEvent(loLogEntry.MessageTemplate, loPropertyIndex, loMetricIndex);
                    if (loLogEntry.MessageTemplate == "Close Application")
                    {
                        loClient.Flush();
                        System.Threading.Thread.Sleep(5000);
                    }
                }
            }
        }

        protected void StartPerformanceCounterLogging()
        {
            if (null == this._oPerformanceModule && null != this.TelemetryConfig)
            {
                this._oPerformanceModule = new PerformanceCollectorModule();
                this._oPerformanceModule.DefaultCounters.Clear();
                this._oPerformanceModule.Counters.Add(new PerformanceCounterCollectionRequest(@"\Process(??APP_WIN32_PROC??)\% Processor Time", @"\Process(??APP_WIN32_PROC??)\% Processor Time"));
                this._oPerformanceModule.Counters.Add(new PerformanceCounterCollectionRequest(@"\Process(??APP_WIN32_PROC??)\% Processor Time Normalized", @"\Process(??APP_WIN32_PROC??)\% Processor Time Normalized"));
                this._oPerformanceModule.Counters.Add(new PerformanceCounterCollectionRequest(@"\Memory\Available Bytes", @"\Memory\Available Bytes"));
                this._oPerformanceModule.Counters.Add(new PerformanceCounterCollectionRequest(@"\Process(??APP_WIN32_PROC??)\Private Bytes", @"\Process(??APP_WIN32_PROC??)\Private Bytes"));
                this._oPerformanceModule.Counters.Add(new PerformanceCounterCollectionRequest(@"\Process(??APP_WIN32_PROC??)\IO Data Bytes/sec", @"\Process(??APP_WIN32_PROC??)\IO Data Bytes/sec"));
                this._oPerformanceModule.Initialize(this.TelemetryConfig);
            }
        }

        protected void StopPerformanceCounterLogging()
        {
            if (null != this._oPerformanceModule)
            {
                this._oPerformanceModule.Dispose();
                this._oPerformanceModule = null;
            }
        }

        /// <summary>
        /// Gets detail about an exception
        /// </summary>
        /// <param name="loException">The exception to get details</param>
        /// <returns>text information about the exception</returns>
        public string GetExceptionDetail(Exception loException)
        {
            return null;
        }

        /// <summary>
        /// Gets information about the current environment
        /// </summary>
        /// <returns>Text based message about the current environment</returns>
        public string GetEnvironmentInformation()
        {
            return null;
        }
    }
}
