package uia.sir.ds.hana;

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
    }
}
