package bixo.storm;

import backtype.storm.topology.base.BaseBasicBolt;

@SuppressWarnings("serial")
public abstract class BaseKafkaBolt extends BaseBasicBolt {

    protected KafkaTopic _publisher;

    public BaseKafkaBolt(KafkaTopic publisher) {
        _publisher = publisher;
    }
    

    @Override
    public void cleanup() {
        if (_publisher != null) {
            _publisher.close();
        }
        
        super.cleanup();
    }
}
