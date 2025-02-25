package uia.sir.ds.mgo;

import uia.sir.ds.ClientFactory;

public class AbstractTest {

    public AbstractTest() throws Exception {
        ClientFactory.register(
                ClientFactory.MGODB,
                "TREK",
                new String[] { "10.160.240.174", "10.160.240.175", "10.160.240.176" },
                new int[] { 27017, 27017, 27017 },
                "trekro",
                "trek2024ro",
                true);
        ClientFactory.register(
                ClientFactory.MGODB,
                "SIR",
                new String[] { "10.160.240.174", "10.160.240.175", "10.160.240.176" },
                new int[] { 27017, 27017, 27017 },
                "sir",
                "sirYes4",
                true);
    }
}
