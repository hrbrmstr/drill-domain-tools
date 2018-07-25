package hrbrmstr.drill.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
// import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
// import org.apache.drill.exec.expr.annotations.Workspace;
// import com.google.common.net.InternetDomainName;

import javax.inject.Inject;

//import com.google.common.collect.ImmutableList;

@FunctionTemplate(
  names = { "suffix_extract" },
  scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class SuffixExtract implements DrillSimpleFunc {
  
  @Param NullableVarCharHolder input;
  
  @Output BaseWriter.ComplexWriter out;
  
  @Inject DrillBuf buffer;
    
  public void setup() {}
  
  public void eval() {
    
    org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mw = out.rootAsMap();
    
    String dom_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
       input.start, input.end, input.buffer
    );
    
    if (dom_string.isEmpty() || dom_string.equals("null")) dom_string = "";
    
    if (com.google.common.net.InternetDomainName.isValid(dom_string)) {
      
      org.apache.drill.exec.expr.holders.VarCharHolder row = 
        new org.apache.drill.exec.expr.holders.VarCharHolder();
      
      com.google.common.net.InternetDomainName parsed = 
        com.google.common.net.InternetDomainName.from(dom_string);

      String normalized = parsed.toString();
      byte[] outBytes;

      if (parsed.hasPublicSuffix()) {

        try {
          String public_suffix = parsed.publicSuffix().toString();
          
          outBytes = public_suffix.getBytes();
          buffer.reallocIfNeeded(outBytes.length); buffer.setBytes(0, outBytes);
          row.start = 0; row.end = outBytes.length; row.buffer = buffer;
          mw.varChar("public_suffix").write(row);
        } catch(IllegalStateException e) {
        }

        if (parsed.hasParent()) {

          try{
            com.google.common.net.InternetDomainName domain = parsed.topPrivateDomain();
            com.google.common.collect.ImmutableList<String> p = domain.parts();

            String tld = p.get(p.size()-1).toString();
            String dom = p.get(0).toString();
            String tp_dom = domain.toString();

            outBytes = tld.getBytes();
            buffer.reallocIfNeeded(outBytes.length); buffer.setBytes(0, outBytes);
            row.start = 0; row.end = outBytes.length; row.buffer = buffer;
            mw.varChar("tld").write(row);

            outBytes = dom.getBytes();
            buffer.reallocIfNeeded(outBytes.length); buffer.setBytes(0, outBytes);
            row.start = 0; row.end = outBytes.length; row.buffer = buffer;
            mw.varChar("domain").write(row);

            outBytes = tp_dom.getBytes();
            buffer.reallocIfNeeded(outBytes.length); buffer.setBytes(0, outBytes);
            row.start = 0; row.end = outBytes.length; row.buffer = buffer;
            mw.varChar("top_private_domain").write(row);

            int loc = normalized.lastIndexOf(tp_dom) - 1;
            if (loc > 0) {
              String host_name = normalized.substring(0, loc);
              outBytes = host_name.getBytes();
              buffer.reallocIfNeeded(outBytes.length); buffer.setBytes(0, outBytes);
              row.start = 0; row.end = outBytes.length; row.buffer = buffer;
              mw.varChar("hostname").write(row);
            } 

          } catch(IllegalStateException e) {
          }

        }

      } else {

        try {
          String host_name = parsed.toString();
          outBytes = host_name.getBytes();
          buffer.reallocIfNeeded(outBytes.length); buffer.setBytes(0, outBytes);
          row.start = 0; row.end = outBytes.length; row.buffer = buffer;
          mw.varChar("hostname").write(row);
        } catch(IllegalStateException e) {
        }

      }
      
    }
    
  }
  
}
