package bixo.storm;

import java.util.EnumSet;

public enum UrlStatus {
    
    UNFETCHED,
    FETCHING,
    FETCHED,
    
    BLOCKED_BY_ROBOTS,            // Blocked by robots.txt
    DEFERRED_BY_ROBOTS,           // Deferred because robots.txt couldn't be processed.

    UNKNOWN_HOST,               // Hostname couldn't be resolved to IP address
    INVALID_URL,                // URL invalid

    HTTP_TEMP_REDIRECTION,      // URL had temp redirect, but that was disallowed in crawl policy
    HTTP_PERM_REDIRECTION,      // URL had perm redirect, but that was disallowed in crawl policy
    HTTP_TOO_MANY_REDIRECTS,
    
    HTTP_CLIENT_ERROR,
    HTTP_UNAUTHORIZED,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
    HTTP_GONE,
        
    HTTP_SERVER_ERROR;
    
    private static final EnumSet<UrlStatus> PERM_ERRORS = EnumSet.of(
        INVALID_URL,
        HTTP_NOT_FOUND,
        HTTP_GONE
        
        // TODO what about UNKNOWN_HOST? Could be a DNS issue
        // TODO what about BLOCKED_BY_ROBOTS? Could get unblocked
    );
    
    public boolean isFetching() {
        return this == FETCHING;
    }
    
    public boolean isPermError() {
        return PERM_ERRORS.contains(this);
    }
    
    public static UrlStatus fromInt(int ordinal) {
        return UrlStatus.values()[ordinal];
    }
}
