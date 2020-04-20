package db.evaluation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public class TimeScaleVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(MongoDBVerticle.class);
  static long start = 0, end = 0;
    PgPool client;
    @Override
  public void start() {
      PgConnectOptions connectOptions = new PgConnectOptions()
          .setPort(5432)
          .setHost("the-host")
          .setDatabase("the-db")
          .setUser("user")
          .setPassword("secret");

      // Pool options
      PoolOptions poolOptions = new PoolOptions()
          .setMaxSize(5);

      // Create the client pool
      client = PgPool.pool(connectOptions, poolOptions);
      logger.info("Timescale Verticle has started!");
      vertx.eventBus().consumer("timescale", message -> search(message));
  }

  private void search(Message<Object> message) {

    try {
        long start = System.currentTimeMillis();
        client
            .query("SELECT row_to_json(*) FROM users WHERE id='julien'")
            .execute(ar -> {
                if (ar.succeeded()) {
                    long end = System.currentTimeMillis();
                    RowSet<Row> result = ar.result();
                    logger.info("Measured time=" + (end - start));
                    JsonArray res = new JsonArray();
                    for(Row row: result)
                        res.add(row);
                    logger.info("Query succeeded with "+ result.size()+ " returned documents in "+(end-start)+" mills.");
                    message.reply(result);
                } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                }
            });
    } catch (Exception e) {
      e.printStackTrace();
      logger.debug(e.getMessage());
      message.reply("failed");
    }
  }
}
