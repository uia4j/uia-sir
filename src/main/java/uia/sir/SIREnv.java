package uia.sir;

import uia.sir.db.dao.DataSourceDao;
import uia.sir.db.dao.SIR;
import uia.sir.db.dao.SIRClient;
import uia.sir.ds.ClientFactory;

public class SIREnv {

    public static void initial() throws Exception {
        SIR.initial(
                new String[] { "10.160.240.174", "10.160.240.175", "10.160.240.176" },
                new int[] { 27017, 27017, 27017 },
                "sir",
                "sirYes4");

        try (SIRClient client = SIR.create()) {
            DataSourceDao dsDao = client.dataSource();
            dsDao.selectAll().forEach(ds -> {
                System.out.printf("%-10s %-10s, %s\n", ds.getType(), ds.getName(), ds.getHosts());

                try {
                    if (ds.getHosts().size() == 1) {
                        ClientFactory.register(
                                ds.getType(),
                                ds.getName(),
                                ds.getHosts().get(0).getAddr(),
                                ds.getHosts().get(0).getPort(),
                                ds.getUsr(),
                                ds.getPwd(),
                                true);
                    }
                    else {
                        String[] hosts = new String[ds.getHosts().size()];
                        int[] ports = new int[ds.getHosts().size()];
                        for (int i = 0; i < hosts.length; i++) {
                            hosts[i] = ds.getHosts().get(i).getAddr();
                            ports[i] = ds.getHosts().get(i).getPort();
                        }
                        ClientFactory.register(
                                ds.getType(),
                                ds.getName(),
                                hosts,
                                ports,
                                ds.getUsr(),
                                ds.getPwd(),
                                true);
                    }
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            });
        }
    }

}
