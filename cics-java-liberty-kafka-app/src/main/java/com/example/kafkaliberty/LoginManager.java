/* Licensed Materials - Property of IBM                               */
/*                                                                    */
/* SAMPLE                                                             */
/*                                                                    */
/* (c) Copyright IBM Corp. 2016, 2025 All Rights Reserved             */
/*                                                                    */
/* US Government Users Restricted Rights - Use, duplication or        */
/* disclosure restricted by GSA ADP Schedule Contract with IBM Corp   */
/*                                                                    */
package com.example.kafkaliberty;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.ibm.websphere.security.auth.callback.WSCallbackHandlerImpl;
import com.ibm.websphere.security.auth.data.AuthData;
import com.ibm.websphere.security.auth.data.AuthDataProvider;


/**
 * LoginManager performs **programmatic JAAS login** using Liberty <authData> credentials.
 *
 * <p>
 * <b>Purpose:</b><br>
 * This class provides an alternative to capturing the HTTP caller's Subject. Instead of
 * using WSSubject.getCallerSubject(), this approach uses credentials stored in server.xml
 * to perform a programmatic login and obtain a Subject.
 * </p>
 *
 * <p>
 * <b>Note:</b> This class is present in the codebase but not active by default.
 * To use it, uncomment the LoginManager injection in KafkaController and modify
 * the start() method to call loginManager.getSubject() instead of WSSubject.getCallerSubject().
 * </p>
 */
public class LoginManager
{
    // Your server.xml <authData id="...">
    private static final String AUTH_DATA_ID = "cicsSAF";

    // Cache the Subject to avoid expensive repeated login
    private volatile Subject cachedSubject;


    /**
     * Returns the programmatically logged-in Subject.
     *
     * <p>
     * Uses double-checked locking for thread-safe lazy initialization. The Subject is
     * cached to avoid expensive repeated JAAS login operations.
     * </p>
     *
     * @return Subject associated with AUTH_DATA_ID
     */
    public Subject getSubject()
    {
        // Double-checked locking pattern for thread-safe lazy initialization
        Subject s = cachedSubject;
        if (s == null)
        {
            synchronized (this)
            {
                if (cachedSubject == null)
                {
                    cachedSubject = loginUsingAuthDataUserPassword(AUTH_DATA_ID);
                }
                s = cachedSubject;
            }
        }
        return s;
    }


    /**
     * Performs JAAS login programmatically using Liberty AuthData credentials.
     *
     * @param alias the <authData> id in server.xml (e.g., "cicsSAF")
     * @return Subject representing the logged-in user
     * @throws RuntimeException if login fails
     */
    private Subject loginUsingAuthDataUserPassword(String alias)
    {
        try
        {
            // Obtain credentials from server.xml <authData> element
            AuthData ad = AuthDataProvider.getAuthData(alias);
            String user = ad.getUserName();

            // Liberty decrypts {aes} password
            char[] pwdChars = ad.getPassword();
            String password = new String(pwdChars);

            // Perform JAAS login using system.DEFAULT configuration
            LoginContext lc = new LoginContext("system.DEFAULT", new WSCallbackHandlerImpl(user, password));
            lc.login();
            return lc.getSubject();
        }
        catch (LoginException e)
        {
            throw new RuntimeException("Programmatic login failed for authData alias '" + alias + "'", e);
        }
    }
}
