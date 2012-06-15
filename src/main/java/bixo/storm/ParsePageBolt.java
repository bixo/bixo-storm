package bixo.storm;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import bixo.config.ParserPolicy;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.parser.BaseContentExtractor;
import bixo.parser.BaseLinkExtractor;
import bixo.parser.HtmlContentExtractor;
import bixo.parser.SimpleLinkExtractor;
import bixo.utils.IoUtils;

@SuppressWarnings("serial")
public class ParsePageBolt implements IRichBolt {

    /**
     * Fixed version of Tika 1.0's IdentityHtmlMapper
     */
    private static class FixedIdentityHtmlMapper extends IdentityHtmlMapper implements Serializable {

        public static final HtmlMapper INSTANCE = new FixedIdentityHtmlMapper();

        @Override
        public String mapSafeElement(String name) {
            return name.toLowerCase(Locale.ENGLISH);
        }
    }

    private transient Parser _parser;
    private transient BaseContentExtractor _contentExtractor;
    private transient BaseLinkExtractor _linkExtractor;
    private transient ParseContext _parseContext;
    private transient OutputCollector _collector;
    
    private IPubSub _publisher;
    
    public ParsePageBolt(IPubSub publisher) {
        super();
        
        _publisher = publisher;
    }
    

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        
        _parseContext = new ParseContext();
        _parseContext.set(HtmlMapper.class, FixedIdentityHtmlMapper.INSTANCE);
        
        _contentExtractor = new HtmlContentExtractor();
        _linkExtractor = new SimpleLinkExtractor();
        
        _parser = new AutoDetectParser();
    }

    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("url");
        Metadata metadata = new Metadata();
        metadata.add(Metadata.RESOURCE_NAME_KEY, url);
        metadata.add(Metadata.CONTENT_TYPE, input.getStringByField("mime-type"));
        // TODO set up language?
        // String charset = CharsetUtils.clean(HttpUtils.getCharsetFromContentType(input.getStringByField("mime-type")));
        // metadata.add(Metadata.CONTENT_LANGUAGE, getLanguage(fetchedDatum, charset));
        
        InputStream is = new ByteArrayInputStream(input.getBinaryByField("content"));

        try {
            URL baseUrl = getContentLocation(url);
            metadata.add(Metadata.CONTENT_LOCATION, baseUrl.toExternalForm());

            // TODO - make TikaCallable in Bixo public class, so we can use it here versus cloning that code.
            Callable<ParsedDatum> c = new TikaCallable(_parser, _contentExtractor, _linkExtractor, is, metadata, false, _parseContext);
            FutureTask<ParsedDatum> task = new FutureTask<ParsedDatum>(c);
            Thread t = new Thread(task);
            t.start();
            
            ParsedDatum result;
            try {
                result = task.get(ParserPolicy.DEFAULT_MAX_PARSE_DURATION, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                task.cancel(true);
                t.interrupt();
                // TODO - handle the exception
                return;
            } finally {
                t = null;
            }
            
            // TODO reuse one tuple for output here.
            _collector.emit("parsed-content", input, Arrays.asList((Object)url, result.getParsedText()));
            
            // TODO reuse one tuple for output here.
            for (Outlink outlink : result.getOutlinks()) {
                _collector.emit("parsed-outlink", input, Arrays.asList((Object)url, outlink.getToUrl()));
            }
        } catch (InterruptedException e) {
            // TODO - handle the exception
            return;
        } catch (ExecutionException e) {
            // TODO - handle the exception
            return;
        } catch (MalformedURLException e) {
            // TODO - handle the exception
            return;
        } finally {
            IoUtils.safeClose(is);
        }
        
    }

    /**
     * Figure out the right base URL to use, for when we need to resolve relative URLs.
     * 
     * @param fetchedDatum
     * @return the base URL
     * @throws MalformedURLException
     */
    protected URL getContentLocation(String fetchedUrl) throws MalformedURLException {
        URL baseUrl = new URL(fetchedUrl);
        
        // See if we have a content location from the HTTP headers that we should use as
        // the base for resolving relative URLs in the document.
        String clUrl = null; // TODO - fetchedDatum.getHeaders().getFirst(HttpHeaderNames.CONTENT_LOCATION);
        if (clUrl != null) {
            // FUTURE KKr - should we try to keep processing if this step fails, but
            // refuse to resolve relative links?
            baseUrl = new URL(baseUrl, clUrl);
        }
        
        return baseUrl;
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     * 
     * We have two streams - parsed content, and extracted outlinks.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("parsed-content", new Fields("url", "content"));
        declarer.declareStream("parsed-outlinks", new Fields("url", "outlink"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // No special configuration
        return null;
    }

}
