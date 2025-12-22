
package com.example.kafkaliberty;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.ibm.websphere.security.auth.callback.WSCallbackHandlerImpl;
import com.ibm.websphere.security.auth.data.AuthData;
import com.ibm.websphere.security.auth.data.AuthDataProvider;

public class LoginManager 
{
    // Your server.xml <authData id="...">
    private static final String AUTH_DATA_ID = "cicsSAF";

    // Cache the Subject to avoid expensive repeated login
    private volatile Subject cachedSubject;

    public Subject getSubject() 
    {
    	// Double-lock pattern for Thread-safety
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
     * JAAS login via system.DEFAULT with WSCallbackHandlerImpl (avoids JCA).
     */
    private Subject loginUsingAuthDataUserPassword(String alias) 
    {
        try 
        {
            // 1) Obtain credentials from server.xml <authData>
            AuthData ad = AuthDataProvider.getAuthData(alias);
            String user = ad.getUserName();
            
            // Liberty decodes the {aes} password generated with securityUtility offline 
            char[] pwdChars = ad.getPassword();         
            String password = new String(pwdChars);

            // 2) Programmatic JAAS login
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
