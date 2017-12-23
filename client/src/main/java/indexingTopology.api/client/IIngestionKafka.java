
package indexingTopology.api.client;

/**
 * Create by zelin on 17-12-20
 **/
interface IIngestionKafka {
    void send(int i, String Msg);
    void ingestProducer();
    void flush();

}
