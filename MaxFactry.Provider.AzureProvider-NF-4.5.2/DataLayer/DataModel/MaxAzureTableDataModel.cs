// <copyright file="MaxAzureTableDataModel.cs" company="Lakstins Family, LLC">
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
// <change date="6/17/2014" author="Brian A. Lakstins" description="Initial Release">
// <change date="8/21/2014" author="Brian A. Lakstins" description="Moved from AzureTable to AzureStorage.">
// <change date="11/30/2018" author="Brian A. Lakstins" description="Updated for changes to base.">
// <change date="3/31/2024" author="Brian A. Lakstins" description="Updated base class.  Updated for changes to base class.">
// <change date="6/4/2025" author="Brian A. Lakstins" description="Use base methods for DataKey and StorageKey">
// </changelog>
#endregion Change Log

namespace MaxFactry.Provider.AzureProvider.DataLayer
{
    using System;
    using MaxFactry.Base.DataLayer;

    /// <summary>
    /// Defines base data model for information stored in an AzureTable
    /// </summary>
    public class MaxAzureTableDataModel : MaxBaseDataModel
    {
        /// <summary>
        /// Partition Key of record
        /// </summary>
        public readonly string PartitionKey = "PartitionKey";

        /// <summary>
        /// Row Key of record
        /// </summary>
        public readonly string RowKey = "RowKey";

        /// <summary>
        /// Timestamp of record
        /// </summary>
        public readonly string Timestamp = "Timestamp";

        /// <summary>
        /// Initializes a new instance of the MaxAzureTableDataModel class
        /// </summary>
        public MaxAzureTableDataModel()
        {
            this.AddStorageKey(this.PartitionKey, typeof(string));
            this.AddDataKey(this.RowKey, typeof(string));
            this.AddType(this.Timestamp, typeof(DateTime));
        }
    }
}