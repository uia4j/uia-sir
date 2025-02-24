package uia.sir;

import uia.sir.ds.ClientFactory;

public class AbstractTest {

    public AbstractTest() throws Exception {
        ClientFactory.register(
                ClientFactory.HANA,
                "KS",
                "10.160.2.38",
                30015,
                "ROAD",
                "Road12345");

        ClientFactory.register(
                ClientFactory.MGODB,
                "TREK",
                new String[] { "10.160.240.174", "10.160.240.175", "10.160.240.176" },
                27017,
                "trekro",
                "trek2024ro");
    }
}
