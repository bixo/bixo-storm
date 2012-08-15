package bixo.storm;

import bixo.fetcher.SimpleHttpFetcher;

public class BixoConfig {

    private CrawlDBClient _cdbClient;
    private SimpleHttpFetcher _pageFetcher;
    private SimpleHttpFetcher _robotsFetcher;
    private DomainResolver _domainResolver;
    
    public BixoConfig(CrawlDBClient cdbClient, SimpleHttpFetcher pageFetcher, SimpleHttpFetcher robotsFetcher, DomainResolver domainResolver) {
        _cdbClient = cdbClient;
        _pageFetcher = pageFetcher;
        _robotsFetcher = robotsFetcher;
        _domainResolver = domainResolver;
    }

    public CrawlDBClient getCdbClient() {
        return _cdbClient;
    }

    public SimpleHttpFetcher getPageFetcher() {
        return _pageFetcher;
    }

    public SimpleHttpFetcher getRobotsFetcher() {
        return _robotsFetcher;
    }

    public DomainResolver getDomainResolver() {
        return _domainResolver;
    }
    
}
