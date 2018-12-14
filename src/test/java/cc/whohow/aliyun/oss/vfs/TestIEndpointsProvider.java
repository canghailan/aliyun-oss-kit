package cc.whohow.aliyun.oss.vfs;

import com.aliyuncs.regions.Endpoint;
import com.aliyuncs.regions.InternalEndpointsParser;
import com.aliyuncs.regions.ProductDomain;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class TestIEndpointsProvider {
    @Test
    public void test() throws Exception {
        InternalEndpointsParser endpointsProvider = new InternalEndpointsParser();

        List<String> endpoint = endpointsProvider.getEndpoints().stream()
                .map(Endpoint::getProductDomains)
                .flatMap(List::stream)
                .filter(self -> "oss".equalsIgnoreCase(self.getProductName()))
                .map(ProductDomain::getDomianName)
                .distinct()
                .collect(Collectors.toList());
        System.out.println(endpoint);
     }
}
