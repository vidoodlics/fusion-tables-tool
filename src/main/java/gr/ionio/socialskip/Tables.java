package gr.ionio.socialskip;

import com.google.api.services.fusiontables.Fusiontables;
import com.google.api.services.fusiontables.model.Column;
import com.google.api.services.fusiontables.model.Table;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * A class for the FusionTables creation and initialization
 * for the SocialSkip Project
 */
public class Tables {
    private FusionTablesAPI api;
    private Fusiontables fusionTables;
    private Logger logger = Logger.getLogger("");



    public Tables() throws Exception {
        this.api = new FusionTablesAPI();
        this.fusionTables = api.getFusionTablesAPI();
    }

    /* Inserts a table in the database */
    public Table insert(Table table) {
        try {
            Table t =  fusionTables.table().insert(table).execute();
            System.out.println(String.format("Inserted: %s => %s\n",t.getName(),t));
            return t;
        }
        catch (Exception e ) {
            logger.info("Exception: "+e);
            return null;
        }
    }

    public FusionTablesAPI getApi() { return this.api; }

    /* Generates the content of the config.xml file */
    public String createXML(String researchers, String exps, String interactions,
                            String accessTokens, String merge, String serviceAccountId) {
        String XML = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<config>\n" +
                "    <fusion_tables>\n" +
                "        <researchers>%s</researchers>\n" +
                "        <experiments>%s</experiments>\n" +
                "        <interactions>%s</interactions>\n" +
                "        <access_tokens>%s</access_tokens>\n" +
                "        <merge_interactions_transactions>%s</merge_interactions_transactions>\n" +
                "    </fusion_tables>\n" +
                "\n" +
                "    <service_account_email>%s</service_account_email>\n" +
                "</config>", researchers, exps, interactions, accessTokens, merge, serviceAccountId);

        return XML;
    }

    /* Each of the following methods generates the corresponding table */

    public Table createResearchers() throws Exception{
        Table researchers = new Table();
        researchers.setName("Researchers");
        researchers.setIsExportable(false);
        researchers.setColumns(Arrays.asList(
                new Column().setName("Mail").setType("STRING"),
                new Column().setName("Name").setType("STRING")
        ));

        Table inserted = insert(researchers);

        return inserted;
    }

    public Table createTransactions() throws Exception {
        Table transactions = new Table();
        transactions.setName("transactions");
        transactions.setIsExportable(false);
        transactions.setColumns(Arrays.asList(
                new Column().setName("Id").setType("NUMBER"),
                new Column().setName("Transaction").setType("STRING")
        ));

        Table inserted = insert(transactions);
        String tableId = inserted.getTableId();

        List<String> queries = Arrays.asList(
                String.format("INSERT INTO %s (Id, Transaction) VALUES (1, 'Backward');", tableId),
                String.format("INSERT INTO %s (Id, Transaction) VALUES (2, 'Forward');", tableId),
                String.format("INSERT INTO %s (Id, Transaction) VALUES (3, 'Play');", tableId),
                String.format("INSERT INTO %s (Id, Transaction) VALUES (4, 'Pause');", tableId));

        for (String q : queries) { fusionTables.query().sql(q).execute();}

        return inserted;
    }

    public Table createExperiments() throws Exception{
        Table experiments = new Table();
        experiments.setName("Experiments");
        experiments.setIsExportable(false);

        experiments.setColumns(Arrays.asList(
                new Column().setName("ResearcherId").setType("NUMBER"),
                new Column().setName("Title").setType("STRING"),
                new Column().setName("VideoURL").setType("STRING"),
                new Column().setName("Questionnaire").setType("STRING"),
                new Column().setName("Info").setType("STRING"),
                new Column().setName("Controls").setType("NUMBER"),
                new Column().setName("TimeRange").setType("NUMBER"),
                new Column().setName("IconColor").setType("STRING"),
                new Column().setName("PgsColor").setType("STRING"),
                new Column().setName("BgColor").setType("STRING")
        ));

        return insert(experiments);
    }

    public Table createInteractions() throws Exception{
        Table interactions = new Table();
        interactions.setName("Interactions");
        interactions.setIsExportable(false);
        interactions.setColumns(Arrays.asList(
                new Column().setName("VideoId").setType("NUMBER"),
                new Column().setName("TesterId").setType("STRING"),
                new Column().setName("TransactionId").setType("NUMBER"),
                new Column().setName("TransactionTime").setType("DATETIME"),
                new Column().setName("Time").setType("NUMBER"),
                new Column().setName("SkipTime").setType("NUMBER")
                )
        );

        return insert(interactions);
    }

    public Table createAccessTokens() throws Exception{
        Table interactions = new Table();
        interactions.setName("Access Tokens");
        interactions.setIsExportable(false);
        interactions.setColumns(Arrays.asList(
                new Column().setName("ResearcherId").setType("NUMBER"),
                new Column().setName("AccessToken").setType("STRING")
        ));

        return insert(interactions);
    }

    /** Merges two tables by creating a view
     * @see {https://support.google.com/fusiontables/answer/171254?hl=en}
     * */
    public Table merge(String name, String t1, String t2, String t1Key, String t2Key) throws Exception {
        String query = String.format("CREATE VIEW %s " +
                        "AS (SELECT * FROM %s AS t1 LEFT OUTER JOIN %s AS t2 " +
                        "ON t1.'%s' = t2.'%s')",
                name, t1, t2, t1Key, t2Key);

        System.out.println(query);

        this.api.getFusionTablesAPI().query().sql(query).execute();

        List<Table> tables = this.api.getFusionTablesAPI().table().list().execute().getItems();

        for (Table t : tables) {
            if (t.getName().equals("MergeOfInteractionsAndTransactions"))
                return t;
        }

        throw new Exception("Table MergeOfInteractionsAndTransactions not found!");
    }

    /* Initializes the SocialSkip database */
    public static String init() throws Exception {
        Tables tables = new Tables();

        Table transactions =  tables.createTransactions();
        Table researchers = tables.createResearchers();
        Table experiments = tables.createExperiments();
        Table interactions = tables.createInteractions();
        Table accessTokens = tables.createAccessTokens();
        Table merged = tables.merge("MergeOfInteractionsAndTransactions",
                interactions.getTableId(), transactions.getTableId(),
                "TransactionId", "Id");


        String XML = tables.createXML(
                researchers.getTableId(),
                experiments.getTableId(),
                interactions.getTableId(),
                accessTokens.getTableId(),
                merged.getTableId(),
                tables.getApi().getCredential().getServiceAccountId());

        return XML;
    }
}
