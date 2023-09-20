// <copyright file="MaxSystemWebSecurityRepositoryAzureTableProvider.cs" company="Lakstins Family, LLC">
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
// <change date="2/24/2014" author="Brian A. Lakstins" description="Initial Release">
// <change date="3/2/2014" author="Brian A. Lakstins" description="Updates to reduce amount of configuration needed.">
// <change date="4/1/2014" author="Brian A. Lakstins" description="Updated to inherit from MaxSystemWebSecurityProvider which implements most methods.">
// <change date="5/7/2014" author="Brian A. Lakstins" description="Fix: Change to partial methods.">
// <change date="5/22/2014" author="Brian A. Lakstins" description="Update to make user and email search case in-sensitive.">
// <change date="6/27/2014" author="Brian A. Lakstins" description="Remove dependency on AppId.">
// <change date="11/11/2014" author="Brian A. Lakstins" description="Update to query for exact matches first, then case-insenstive matches.">
// <change date="6/4/2015" author="Brian A. Lakstins" description="Update for new security module.">
// </changelog>
#endregion

namespace MaxFactry.General.DataLayer.Provider
{
	using System;
    using MaxFactry.Core;
    using MaxFactry.Base.DataLayer;
    using MaxFactry.Base.DataLayer.Provider;

	/// <summary>
	/// Provider for all Membership repositories
	/// </summary>
    public class MaxSecurityRepositoryAzureTableProvider : MaxSecurityRepositoryDefaultProvider
	{
        /// <summary>
        /// Initializes the provider.
        /// </summary>
        /// <param name="lsName">Name of the provider.</param>
        /// <param name="loConfig">Configuration information.</param>
        public override void Initialize(string lsName, MaxIndex loConfig)
        {
            this.DefaultContextProviderType = typeof(MaxDataContextAzureTableProvider);
            base.Initialize(lsName, loConfig);
        }

        /// <summary>
        /// Selects all users that match the given username.
        /// </summary>
        /// <param name="loData">The user data.</param>
        /// <param name="lsUserName">The username of the user.</param>
        /// <param name="lnPageIndex">Page of data to select.</param>
        /// <param name="lnPageSize">Size of the page to select.</param>
        /// <param name="lsSort">Sort information.</param>
        /// <param name="lnTotal">Total matching records.</param>
        /// <returns>List of users.</returns>
        public override MaxDataList SelectAllUserByUserName(MaxData loData, string lsUserName, int lnPageIndex, int lnPageSize, string lsSort, out int lnTotal)
        {
            MaxUserDataModel loDataModel = loData.DataModel as MaxUserDataModel;
            if (null == loDataModel)
            {
                throw new MaxException("Error casting [" + loData.DataModel.GetType() + "] for DataModel");
            }

            MaxDataQuery loDataQuery = new MaxDataQuery();
            loDataQuery.AddFilter(loDataModel.IsDeleted, "=", false);
            if (!string.IsNullOrEmpty(lsUserName))
            {
                loDataQuery.AddCondition("AND");
                loDataQuery.AddFilter(loDataModel.UserName, "=", lsUserName);

                // Get all the users that match the username exactly.
                MaxDataList loDataForNameList = this.Select(loData, loDataQuery, lnPageIndex, lnPageSize, lsSort, out lnTotal);
                if (loDataForNameList.Count > 0)
                {
                    return loDataForNameList;
                }
            }

            // Username storage is case sensitive, but lookups should not need to be, so pull all users and match username case insensitive
            loDataQuery = new MaxDataQuery();
            loDataQuery.AddFilter(loDataModel.IsDeleted, "=", false);
            MaxDataList loDataAllList = this.Select(loData, loDataQuery, lnPageIndex, lnPageSize, lsSort, out lnTotal);
            MaxDataList loR = new MaxDataList(loDataModel);
            //// Filter by username
            lnTotal = 0;
            for (int lnD = 0; lnD < loDataAllList.Count; lnD++)
            {
                MaxData loDataOut = loDataAllList[lnD];
                bool lbIsMatch = false;
                if (string.IsNullOrEmpty(lsUserName))
                {
                    lbIsMatch = true;
                }

                if (!lbIsMatch)
                {
                    string lsUserNameCheck = MaxConvertLibrary.ConvertToString(typeof(object), loDataOut.Get(loDataModel.UserName));
                    if (lsUserNameCheck.Equals(lsUserName, StringComparison.InvariantCultureIgnoreCase))
                    {
                        lbIsMatch = true;
                    }
                }

                if (lbIsMatch)
                {
                    lnTotal += 1;
                    loR.Add(loDataOut);
                }
            }

            return loR;
        }

        /// <summary>
        /// Selects all users that match the given username.
        /// </summary>
        /// <param name="loData">The user data.</param>
        /// <param name="lsEmail">The email of the user.</param>
        /// <param name="lnPageIndex">Page of data to select.</param>
        /// <param name="lnPageSize">Size of the page to select.</param>
        /// <param name="lsSort">Sort information.</param>
        /// <param name="lnTotal">Total matching records.</param>
        /// <returns>List of users.</returns>
        public override MaxDataList SelectAllUserByEmail(MaxData loData, string lsEmail, int lnPageIndex, int lnPageSize, string lsSort, out int lnTotal)
        {
            MaxUserDataModel loDataModel = loData.DataModel as MaxUserDataModel;
            if (null == loDataModel)
            {
                throw new MaxException("Error casting [" + loData.DataModel.GetType() + "] for DataModel");
            }

            MaxDataQuery loDataQuery = new MaxDataQuery();
            loDataQuery.AddFilter(loDataModel.IsDeleted, "=", false);
            loDataQuery.AddCondition("AND");
            loDataQuery.AddFilter(loDataModel.Email, "=", lsEmail);

            // Get all the users that match the email exactly.
            MaxDataList loDataList = this.Select(loData, loDataQuery, lnPageIndex, lnPageSize, lsSort, out lnTotal);
            if (loDataList.Count > 0)
            {
                return loDataList;
            }

            // Email storage is case sensitive, but lookups should not need to be, so pull all users and match username case insensitive
            loDataQuery = new MaxDataQuery();
            loDataQuery.AddFilter(loDataModel.IsDeleted, "=", false);
            MaxDataList loDataAllList = this.Select(loData, loDataQuery, lnPageIndex, lnPageSize, lsSort, out lnTotal);

            // Filter by email
            lnTotal = 0;
            for (int lnD = 0; lnD < loDataAllList.Count; lnD++)
            {
                MaxData loDataOut = loDataAllList[lnD];
                string lsEmailCheck = MaxConvertLibrary.ConvertToString(typeof(object), loDataOut.Get(loDataModel.Email));
                if (lsEmailCheck.Equals(lsEmail, StringComparison.InvariantCultureIgnoreCase))
                {
                    lnTotal += 1;
                    loDataList.Add(loDataOut);
                }
            }

            return loDataList;
        }

        /// <summary>
        /// Selects all users that match the given username.
        /// </summary>
        /// <param name="loData">The user data.</param>
        /// <param name="lsUserName">The username of the user.</param>
        /// <param name="lnPageIndex">Page of data to select.</param>
        /// <param name="lnPageSize">Size of the page to select.</param>
        /// <param name="lsSort">Sort information.</param>
        /// <param name="lnTotal">Total matching records.</param>
        /// <returns>List of users.</returns>
        public override MaxDataList SelectAllUserByUserNamePartial(MaxData loData, string lsUserName, int lnPageIndex, int lnPageSize, string lsSort, out int lnTotal)
        {
            MaxUserDataModel loDataModel = loData.DataModel as MaxUserDataModel;
            if (null == loDataModel)
            {
                throw new MaxException("Error casting [" + loData.DataModel.GetType() + "] for DataModel");
            }

            MaxDataQuery loDataQuery = new MaxDataQuery();
            loDataQuery.AddFilter(loDataModel.IsDeleted, "=", false);

            // Get all the users
            MaxDataList loDataAllList = this.Select(loData, loDataQuery, lnPageIndex, lnPageSize, lsSort, out lnTotal);
            
            // Filter by username
            MaxDataList loDataList = new MaxDataList(loDataModel);
            lnTotal = 0;
            for (int lnD = 0; lnD < loDataAllList.Count; lnD++)
            {
                MaxData loDataOut = loDataAllList[lnD];
                string lsUserNameCheck = MaxConvertLibrary.ConvertToString(typeof(object), loDataOut.Get(loDataModel.UserName));
                if (lsUserNameCheck.ToLowerInvariant().Contains(lsUserName.ToLowerInvariant()))
                {
                    lnTotal += 1;
                    loDataList.Add(loDataOut);
                }
            }

            return loDataList;
        }

        /// <summary>
        /// Selects all users that match the given email.
        /// </summary>
        /// <param name="loData">The user data.</param>
        /// <param name="lsEmail">The email of the user.</param>
        /// <param name="lnPageIndex">Page of data to select.</param>
        /// <param name="lnPageSize">Size of the page to select.</param>
        /// <param name="lsSort">Sort information.</param>
        /// <param name="lnTotal">Total matching records.</param>
        /// <returns>List of users.</returns>
        public override MaxDataList SelectAllUserByEmailPartial(MaxData loData, string lsEmail, int lnPageIndex, int lnPageSize, string lsSort, out int lnTotal)
        {
            MaxUserDataModel loDataModel = loData.DataModel as MaxUserDataModel;
            if (null == loDataModel)
            {
                throw new MaxException("Error casting [" + loData.DataModel.GetType() + "] for DataModel");
            }

            MaxDataQuery loDataQuery = new MaxDataQuery();
            loDataQuery.AddFilter(loDataModel.IsDeleted, "=", false);

            // Get all the users that match the App Id.
            MaxDataList loDataAllList = this.Select(loData, loDataQuery, lnPageIndex, lnPageSize, lsSort, out lnTotal);
            
            // Filter by email
            MaxDataList loDataList = new MaxDataList(loDataModel);
            lnTotal = 0;
            for (int lnD = 0; lnD < loDataAllList.Count; lnD++)
            {
                MaxData loDataOut = loDataAllList[lnD];
                string lsEmailCheck = MaxConvertLibrary.ConvertToString(typeof(object), loDataOut.Get(loDataModel.Email));
                if (lsEmailCheck.ToLowerInvariant().Contains(lsEmail.ToLowerInvariant()))
                {
                    lnTotal += 1;
                    loDataList.Add(loDataOut);
                }
            }

            return loDataList;
        }

        /// <summary>
        /// Gets the count of users that match the username filter.
        /// </summary>
        /// <param name="loData">The user data.</param>
        /// <param name="lsUserName">The username of the user.</param>
        /// <returns>Count of users.</returns>
        public override int GetUserCountByUserName(MaxData loData, string lsUserName)
        {
            MaxUserDataModel loDataModel = loData.DataModel as MaxUserDataModel;
            if (null == loDataModel)
            {
                throw new MaxException("Error casting [" + loData.DataModel.GetType() + "] for DataModel");
            }

            int lnR = 0;
            MaxDataQuery loDataQuery = new MaxDataQuery();
            loDataQuery.AddFilter(loDataModel.IsDeleted, "=", false);

            // Get all the users that match the App Id.
            MaxDataList loDataAllList = this.Select(loData, loDataQuery, 0, 0, string.Empty, out lnR);
            
            // Filter by username
            lnR = 0;
            for (int lnD = 0; lnD < loDataAllList.Count; lnD++)
            {
                MaxData loDataOut = loDataAllList[lnD];
                string lsUserNameCheck = MaxConvertLibrary.ConvertToString(typeof(object), loDataOut.Get(loDataModel.UserName));
                if (lsUserNameCheck.ToLowerInvariant().Contains(lsUserName.ToLowerInvariant()))
                {
                    lnR += 1;
                }
            }

            return lnR;
        }
	}
}
