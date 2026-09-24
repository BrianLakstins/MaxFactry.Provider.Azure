// <copyright file="MaxTokenLibraryAzureProvider.cs" company="Lakstins Family, LLC">
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
// <change date="9/24/2026" author="Brian A. Lakstins" description="Initial creation">
// </changelog>
#endregion

namespace MaxFactry.General.BusinessLayer.Provider
{
    using System;
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using JWT;
    using JWT.Serializers;
    using MaxFactry.Core;

    /// <summary>
    /// Provides methods to handle business layer security
    /// </summary>
    public class MaxTokenLibraryAzureProvider : MaxTokenLibraryDefaultProvider
    {
        public override MaxIndex ParseToken(string lsToken)
        {
            MaxIndex loR = new MaxIndex();
            loR.Add("TokenText", lsToken);
            Regex loJWTRegex = new Regex(@"^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$");
            if (loJWTRegex.IsMatch(lsToken))
            {
                try
                {
                    IJsonSerializer loSerializer = new JsonNetSerializer();
                    IBase64UrlEncoder loUrlEncoder = new JwtBase64UrlEncoder();
                    IJwtDecoder loDecoder = new JwtDecoder(loSerializer, loUrlEncoder);
                    IDictionary<string, object> loToken = loDecoder.DecodeToObject<IDictionary<string, object>>(lsToken, string.Empty, false);
                    foreach (string lsKey in loToken.Keys)
                    {
                        loR.Add(lsKey, loToken[lsKey]);
                    }
                }
                catch (Exception loE)
                {
                    MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "ParseToken", MaxEnumGroup.LogError, "Exception parsing toaken {lsToken}", loE, lsToken));
                }
            }
            else
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "ParseToken", MaxEnumGroup.LogStatic, "Token does not match JWT format {lsIdToken}", lsToken));
            }

            return loR;
        }

        public override bool IsValidToken(MaxIndex loToken)
        {
            bool lbR = false;
            try
            {
                string lsTenantId = loToken["tid"] as string;
                object loTenantList = MaxConfigurationLibrary.GetValue(MaxEnumGroup.ScopeApplication, "OAuth2OIDCMicrosoftTenantList");
                if (null != loTenantList)
                {
                    string lsTenantList = MaxConvertLibrary.ConvertToString(typeof(object), loTenantList).ToLower();
                    if (lsTenantList.Contains(lsTenantId))
                    {
                        //// Check the audience
                        string lsAudienceId = loToken["aud"] as string;
                        object loAudienceList = MaxConfigurationLibrary.GetValue(MaxEnumGroup.ScopeApplication, "OAuth2OIDCMicrosoftAudienceList");
                        if (null != loAudienceList)
                        {
                            string lsAudienceList = MaxConvertLibrary.ConvertToString(typeof(object), loAudienceList).ToLower();
                            if (lsAudienceList.Contains(lsAudienceId))
                            {
                                // Fetch Microsoft's public keys
                                string lsKeysUrl = string.Format("https://login.microsoftonline.com/{0}/discovery/v2.0/keys", lsTenantId);
                                System.Net.WebClient loClient = new System.Net.WebClient();
                                string lsKeysJson = loClient.DownloadString(lsKeysUrl);

                                // Parse the keys JSON
                                IJsonSerializer loSerializer = new JsonNetSerializer();
                                IDictionary<string, object> loKeysResponse = loSerializer.Deserialize<IDictionary<string, object>>(lsKeysJson);

                                // Get the token header to find the key id (kid)
                                string[] laTokenParts = loToken.GetValueString("TokenText").Split('.');
                                if (laTokenParts.Length == 3)
                                {
                                    IBase64UrlEncoder loUrlEncoder = new JwtBase64UrlEncoder();
                                    string lsHeaderJson = System.Text.Encoding.UTF8.GetString(loUrlEncoder.Decode(laTokenParts[0]));
                                    IDictionary<string, object> loHeader = loSerializer.Deserialize<IDictionary<string, object>>(lsHeaderJson);
                                    string lsKid = loHeader["kid"] as string;

                                    // Find the matching key
                                    Newtonsoft.Json.Linq.JArray loKeys = loKeysResponse["keys"] as Newtonsoft.Json.Linq.JArray;
                                    foreach (Newtonsoft.Json.Linq.JObject loKey in loKeys)
                                    {
                                        if (loKey["kid"].ToString() == lsKid)
                                        {
                                            string lsModulus = loKey["n"].ToString();
                                            string lsExponent = loKey["e"].ToString();

                                            // Create RSA parameters from the key
                                            System.Security.Cryptography.RSAParameters loRsaParams = new System.Security.Cryptography.RSAParameters
                                            {
                                                Modulus = loUrlEncoder.Decode(lsModulus),
                                                Exponent = loUrlEncoder.Decode(lsExponent)
                                            };

                                            // Create RSA provider and verify signature
                                            using (System.Security.Cryptography.RSACryptoServiceProvider loRsa = new System.Security.Cryptography.RSACryptoServiceProvider())
                                            {
                                                loRsa.ImportParameters(loRsaParams);

                                                // Get the signature and data to verify
                                                byte[] laSignature = loUrlEncoder.Decode(laTokenParts[2]);
                                                byte[] laDataToVerify = System.Text.Encoding.UTF8.GetBytes(laTokenParts[0] + "." + laTokenParts[1]);

                                                // Verify using SHA256
                                                using (System.Security.Cryptography.SHA256 loSha256 = System.Security.Cryptography.SHA256.Create())
                                                {
                                                    byte[] laHash = loSha256.ComputeHash(laDataToVerify);
                                                    lbR = loRsa.VerifyHash(laHash, System.Security.Cryptography.CryptoConfig.MapNameToOID("SHA256"), laSignature);
                                                }
                                            }

                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception loE)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "IsValidToken", MaxEnumGroup.LogError, "Error validating token {loToken}", loE, loToken));
            }

            return lbR;
        }

        public override bool ValidateTokenSignature(MaxIndex loToken)
        {
            bool lbR = false;
            try
            {
                string lsTenantId = loToken["tid"] as string;
                // Fetch Microsoft's public keys
                string lsKeysUrl = string.Format("https://login.microsoftonline.com/{0}/discovery/v2.0/keys", lsTenantId);
                System.Net.WebClient loClient = new System.Net.WebClient();
                string lsKeysJson = loClient.DownloadString(lsKeysUrl);

                // Parse the keys JSON
                IJsonSerializer loSerializer = new JsonNetSerializer();
                IDictionary<string, object> loKeysResponse = loSerializer.Deserialize<IDictionary<string, object>>(lsKeysJson);

                // Get the token header to find the key id (kid)
                string[] laTokenParts = loToken.GetValueString("TokenText").Split('.');
                if (laTokenParts.Length == 3)
                {
                    IBase64UrlEncoder loUrlEncoder = new JwtBase64UrlEncoder();
                    string lsHeaderJson = System.Text.Encoding.UTF8.GetString(loUrlEncoder.Decode(laTokenParts[0]));
                    IDictionary<string, object> loHeader = loSerializer.Deserialize<IDictionary<string, object>>(lsHeaderJson);
                    string lsKid = loHeader["kid"] as string;

                    // Find the matching key
                    Newtonsoft.Json.Linq.JArray loKeys = loKeysResponse["keys"] as Newtonsoft.Json.Linq.JArray;
                    foreach (Newtonsoft.Json.Linq.JObject loKey in loKeys)
                    {
                        if (loKey["kid"].ToString() == lsKid)
                        {
                            string lsModulus = loKey["n"].ToString();
                            string lsExponent = loKey["e"].ToString();

                            // Create RSA parameters from the key
                            System.Security.Cryptography.RSAParameters loRsaParams = new System.Security.Cryptography.RSAParameters
                            {
                                Modulus = loUrlEncoder.Decode(lsModulus),
                                Exponent = loUrlEncoder.Decode(lsExponent)
                            };

                            // Create RSA provider and verify signature
                            using (System.Security.Cryptography.RSACryptoServiceProvider loRsa = new System.Security.Cryptography.RSACryptoServiceProvider())
                            {
                                loRsa.ImportParameters(loRsaParams);

                                // Get the signature and data to verify
                                byte[] laSignature = loUrlEncoder.Decode(laTokenParts[2]);
                                byte[] laDataToVerify = System.Text.Encoding.UTF8.GetBytes(laTokenParts[0] + "." + laTokenParts[1]);

                                // Verify using SHA256
                                using (System.Security.Cryptography.SHA256 loSha256 = System.Security.Cryptography.SHA256.Create())
                                {
                                    byte[] laHash = loSha256.ComputeHash(laDataToVerify);
                                    lbR = loRsa.VerifyHash(laHash, System.Security.Cryptography.CryptoConfig.MapNameToOID("SHA256"), laSignature);
                                }
                            }

                            break;
                        }
                    }
                }
            }
            catch (Exception loE)
            {
                MaxLogLibrary.Log(new MaxLogEntryStructure(this.GetType(), "ValidateTokenSignature", MaxEnumGroup.LogError, "Error validating token signature {loToken}", loE, loToken));
            }

            return lbR;
        }
    }
}
