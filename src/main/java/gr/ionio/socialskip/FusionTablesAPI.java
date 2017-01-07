package gr.ionio.socialskip;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.fusiontables.Fusiontables;
import com.google.api.services.fusiontables.FusiontablesScopes;
import com.google.api.services.fusiontables.model.Table;
import com.google.api.services.fusiontables.model.TableList;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

/**
 * A class for the initialization of the Fusiontables API
 * */

public class FusionTablesAPI {
    private HttpTransport HTTP_TRANSPORT;
    private JsonFactory JSON_FACTORY;
    private GoogleCredential credential;
    private Fusiontables fusiontables;

    public FusionTablesAPI() throws Exception {
        HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        JSON_FACTORY = JacksonFactory.getDefaultInstance();
        InputStream is = FusionTablesAPI.class.getResourceAsStream("/key.json");
        credential = GoogleCredential
                .fromStream(is)
                .createScoped(Collections.singleton(FusiontablesScopes.FUSIONTABLES));
        this.fusiontables = new Fusiontables(HTTP_TRANSPORT, JSON_FACTORY, credential);
    }

    public Fusiontables getFusionTablesAPI() {
        return this.fusiontables;
    }

    public GoogleCredential getCredential() { return this.credential; }

    /* Deletes all tables in the database */
    public void dropAllTables() throws Exception {
        TableList tableList = fusiontables.table().list().execute();

        if (tableList != null && tableList.getItems() != null) {
            for (Table t : tableList.getItems()) {
                fusiontables.table().delete(t.getTableId()).execute();
            }
        }
    }

    /* Lists all tables in the database */
    public void listAllTables() throws Exception {
        for (Table t : fusiontables.table().list().execute().getItems()) {
            System.out.println("Name: "+t.getName());
            System.out.println("Cols: "+t.get("columns"));

            List<List<Object>> rows = fusiontables.query()
                    .sql("SELECT * FROM "+t.getTableId()).execute().getRows();

            if (rows != null && !rows.isEmpty()) {
                for (List list : rows) {
                    System.out.println(list);
                }
            }
        }
    }
}
