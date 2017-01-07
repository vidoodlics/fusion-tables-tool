package gr.ionio.socialskip;

import java.io.File;
import java.io.FileOutputStream;

/**
 * Created by vic on 3/1/2017.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Tables tables = new Tables();
        String XML = tables.init();

        FileOutputStream out = new FileOutputStream(new File("conf.xml"));
        out.write(XML.getBytes());
        out.close();
    }
}
