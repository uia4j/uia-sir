package uia.sir.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DataSource extends Mgo {

    private String name;

    private String type;

    private List<Host> hosts;

    private String usr;

    private String pwd;

    private Map<String, String> props;

    private List<String> accumulators;

    public DataSource() {
        this.type = "MGODB";
        this.hosts = new ArrayList<>();
        this.props = new TreeMap<>();
        this.accumulators = new ArrayList<>();
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Host> getHosts() {
        return this.hosts;
    }

    public void setHosts(List<Host> hosts) {
        this.hosts = hosts;
    }

    public String getUsr() {
        return this.usr;
    }

    public void setUsr(String usr) {
        this.usr = usr;
    }

    public String getPwd() {
        return this.pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public Map<String, String> getProps() {
        return this.props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public List<String> getAccumulators() {
        return this.accumulators;
    }

    public void setAccumulators(List<String> accumulators) {
        this.accumulators = accumulators;
    }

    public static class Host {

        private String addr;

        private int port;

        public Host() {
        }

        public Host(String addr, int port) {
            this.addr = addr;
            this.port = port;
        }

        public String getAddr() {
            return this.addr;
        }

        public void setAddr(String addr) {
            this.addr = addr;
        }

        public int getPort() {
            return this.port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public String toString() {
            return this.addr;
        }

    }
}
