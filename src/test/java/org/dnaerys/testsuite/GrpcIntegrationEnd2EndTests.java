package org.dnaerys.testsuite;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import org.dnaerys.cluster.grpc.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Disabled;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Integration end-to-end tests")
public class GrpcIntegrationEnd2EndTests {

    // Technically, this is not unit tests in a purist sense, but rather integration end-to-end tests.
    // Tests send gRPC requests to externally running cluster with concrete data in it - Ashkenazi Jewish trio.
    //
    // Dataset:
    // NIST Reference samples, Ashkenazi Jewish family, HG002/HG003/HG004, aligned to GRCh37
    // Genome in a Bottle: https://www.nist.gov/programs-projects/genome-bottle
    //                     https://www.nature.com/articles/sdata201625/tables/3

    private static final Logger logger = Logger.getLogger(GrpcIntegrationEnd2EndTests.class.getName());
    private static DnaerysServiceGrpc.DnaerysServiceBlockingStub blockingStub;
    private static ManagedChannel channel;

    public GrpcIntegrationEnd2EndTests() {
    }

    @BeforeAll
    public static void startUp() {
        String target = "localhost:8001";
//        String target = "192.168.49.2:30383"; // k8s
        logger.info("Testing server: " + target);
        logger.info("Starting up a client and opening a channel...");
        // Creates a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        channel = ManagedChannelBuilder.forTarget(target)
            // Channels are secure by default (via SSL/TLS). For these tests TLS is disabled to avoid certificates.
            .usePlaintext()
            .build();
        blockingStub = DnaerysServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        logger.info("Shutting down client...");
        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        channel
            .shutdownNow()
            .awaitTermination(5, TimeUnit.SECONDS);
    }

    // --- Cluster status ----------------------------------------------------------------------------------------------

    @Nested
    @DisplayName("Cluster status")
    class ClusterStatus {
        @Test
        @DisplayName("Cluster Health Check status")
        public void healthStatus() {
            HealthRequest request = HealthRequest.newBuilder().build();
            HealthResponse response;
            response = blockingStub.health(request);
            logger.info("Health status: " + response.getStatus());
            assertEquals("200 OK", response.getStatus());
        }

        @Test
        @DisplayName("Cluster Active Nodes")
        public void activeNodes() {
            ClusterNodesRequest request = ClusterNodesRequest.newBuilder().build();
            ClusterNodesResponse response;
            response = blockingStub.clusterNodes(request);
            logger.info("Total number of active nodes in cluster: " + response.getActiveNodesCount());
            logger.info("Total number of inactive nodes in cluster: " + response.getInactiveNodesCount());
            logger.info("Nodes: " + response.getTotalNodes());
            assertTrue(response.getTotalNodes() > 0);
        }
    }

    // --- Beacon ------------------------------------------------------------------------------------------------------

    @Nested
    @DisplayName("Beacon")
    class Beacon {
        @Test
        @DisplayName("Beacon request for a single SNP")
        public void beacon() {
            final Chromosome chr = Chromosome.CHR_X;
            final int pos = 155004280;
            final String alt = "G";
            BeaconResponse response;
            RefAssembly assembly = RefAssembly.GRCh37;
            BeaconRequest request =
                BeaconRequest
                    .newBuilder()
                    .setChr(chr)
                    .setPos(pos)
                    .setAlt(alt)
                    .setAssembly(assembly)
                    .build();
            logger.info("Beacon test for " + chr + " pos: " + pos + " alt: " + alt + " assembly: " + assembly);
            response = blockingStub.beacon(request);
            assertTrue(response.getExists());
            assertAll(() -> assertEquals(1.0, response.getAc(), 0.01f),
                      () -> assertEquals(0.167, response.getAf(), 0.001f));
        }

        @Test
        @DisplayName("Beacon assembly mismatch")
        public void beaconAssemblyMismatch() {
            final Chromosome chr = Chromosome.CHR_X;
            final int pos = 155004280;
            final String alt = "G";
            BeaconResponse response;
            RefAssembly assembly = RefAssembly.GRCh38;
            BeaconRequest request =
                BeaconRequest
                    .newBuilder()
                    .setChr(chr)
                    .setPos(pos)
                    .setAlt(alt)
                    .setAssembly(assembly)
                    .build();
            logger.info("Beacon test for assembly mismatch");
            response = blockingStub.beacon(request);
            assertFalse(response.getExists());
        }

        @Test
        @DisplayName("Beacon empty results for default assembly")
        public void beaconDefaultAssembly() {
            final Chromosome chr = Chromosome.CHR_X;
            final int pos = 155004280;
            final String alt = "G";
            BeaconResponse response;
            BeaconRequest request =
                BeaconRequest
                    .newBuilder()
                    .setChr(chr)
                    .setPos(pos)
                    .setAlt(alt)
                    .build();
            logger.info("Beacon test for " + chr + " pos: " + pos + " alt: " + alt + " assembly: default");
            response = blockingStub.beacon(request);
            assertFalse(response.getExists());
        }

        @Test
        @DisplayName("Beacon with empty chromosome")
        public void beaconEmptyChr() {
            final Chromosome chr = Chromosome.CHR_MT;
            final int pos = 1;
            final String alt = "A";
            BeaconResponse response;
            RefAssembly assembly = RefAssembly.GRCh37;
            BeaconRequest request =
                BeaconRequest
                    .newBuilder()
                    .setChr(chr)
                    .setPos(pos)
                    .setAlt(alt)
                    .setAssembly(assembly)
                    .build();
            logger.info("Beacon with empty chromosome");
            response = blockingStub.beacon(request);
            assertFalse(response.getExists());
        }

        @Test
        @DisplayName("Beacon empty results")
        public void beaconEmpty() {
            final Chromosome chr = Chromosome.CHR_X;
            final int pos = 155004280;
            final String alt = "A";
            BeaconResponse response;
            RefAssembly assembly = RefAssembly.GRCh37;
            BeaconRequest request =
                BeaconRequest
                    .newBuilder()
                    .setChr(chr)
                    .setPos(pos)
                    .setAlt(alt)
                    .setAssembly(assembly)
                    .build();
            logger.info("Beacon test for " + chr + " pos: " + pos + " alt: " + alt + " - must be empty");
            response = blockingStub.beacon(request);
            assertFalse(response.getExists());
        }
    }

    // --- Select variants in a single region --------------------------------------------------------------------------

    @Nested
    @DisplayName("SelectVariantsInRegion")
    class SelectVariantsInRegion {

        @Test
        @DisplayName("Select SNP")
        public void selectSNPInRegion() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880200;
            final int end = 880238;
            final boolean selectHom = true;
            final boolean selectHet = true;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: select a single SNP: " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegion(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesResponse allelesResponse = response.next();
            assertEquals(1, allelesResponse.getAllelesCount());
            Variant variant = allelesResponse.getAlleles(0);
            // verify 1:880238-880238:A->G
            assertAll(
                () -> assertTrue(chr.equals(variant.getChr())),
                () -> assertEquals(end, variant.getStart()),
                () -> assertEquals(end, variant.getEnd()),
                () -> assertEquals("A", variant.getRef()),
                () -> assertEquals("G", variant.getAlt()),
                () -> assertEquals(1.0f, variant.getAf(), 0.001f),
                () -> assertEquals(6.0f, variant.getAc(), 0.001f),
                () -> assertEquals(6.0f, variant.getAn(), 0.001f),
                () -> assertEquals(3, variant.getHomc()),
                () -> assertEquals(0, variant.getHetc()),
                () -> assertEquals(0, variant.getMisc()));
        }

        @Test
        @DisplayName("Select Hom variants, single SNP")
        public void selectHomVariantsInRegion() {
            final Chromosome chr = Chromosome.CHR_X;
            final int start = 155004280;
            final int end = 155270560; // end of chrX
            final boolean selectHom = true;
            final boolean selectHet = false;
            final int expectedAlleles = 1;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: select Hom variants: " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegion(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesResponse allelesResponse = response.next();
            assertEquals(expectedAlleles, allelesResponse.getAllelesCount());
            Variant variant = allelesResponse.getAlleles(0);
            // verify X:155237350-155237351:AC->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getChr())),
                () -> assertEquals(155237350, variant.getStart()),
                () -> assertEquals(155237351, variant.getEnd()),
                () -> assertEquals("AC", variant.getRef()),
                () -> assertEquals("A", variant.getAlt()),
                () -> assertEquals(1.0f, variant.getAf(), 0.001f),
                () -> assertEquals(6.0f, variant.getAc(), 0.001f),
                () -> assertEquals(6, variant.getAn()),
                () -> assertEquals(2, variant.getHomc()), // hom alleles in males
                () -> assertEquals(0, variant.getHetc()),
                () -> assertEquals(0, variant.getMisc()),
                () -> assertEquals(1, variant.getHomfc())); // hom alleles in females
        }

        @Test
        @DisplayName("Select Het variants, single SNP")
        public void selectHetVariantsInRegion2() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880238;
            final int end = 880391;
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedAlleles = 1;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: select Het variants in region " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegion(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesResponse allelesResponse = response.next();
            assertEquals(expectedAlleles, allelesResponse.getAllelesCount());
            Variant variant = allelesResponse.getAlleles(0);
            // verify 1:880390-880390:C->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getChr())),
                () -> assertEquals(880390, variant.getStart()),
                () -> assertEquals(880390, variant.getEnd()),
                () -> assertEquals("C", variant.getRef()),
                () -> assertEquals("A", variant.getAlt()),
                () -> assertEquals(0.16666667f, variant.getAf(), 0.001f),
                () -> assertEquals(1f, variant.getAc(), 0.001f),
                () -> assertEquals(6, variant.getAn()),
                () -> assertEquals(0, variant.getHomc()),
                () -> assertEquals(1, variant.getHetc()),
                () -> assertEquals(0, variant.getMisc()));
        }

        @Test
        @DisplayName("Select Het variants")
        public void selectHetVariantsInRegion() {
            final Chromosome chr = Chromosome.CHR_X;
            final int start = 155004280;
            final int end = 155270560; // end of chr X
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedAlleles = 3;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: select Het variants in region " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegion(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select all p53 variants")
        public void selectVariantsInP53() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedAlleles = 7;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: select p53 variants: " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegion(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with assembly mismatch")
        public void selectVariantsAssemblyMismatch() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh38;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: with assembly mismatch");
            response = blockingStub.selectVariantsInRegion(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with default assembly")
        public void selectVariantsDefaultAssembly() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedAlleles = 0;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .build();
            logger.info("SelectVariantsInRegion: with default assembly");
            response = blockingStub.selectVariantsInRegion(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with empty chromosome")
        public void selectVariantsEmptyChr() {
            final Chromosome chr = Chromosome.CHR_MT;
            final int start = 1;
            final int end = 1;
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: with empty chromosome");
            response = blockingStub.selectVariantsInRegion(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select empty results")
        public void selectVariantsInRegionEmpty() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 1; // start of chr1
            final int end = 249250621; // end of chr1
            final boolean selectHom = false;
            final boolean selectHet = false;
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionRequest request =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: select whole chr 1 with empty result");
            response = blockingStub.selectVariantsInRegion(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }
    }

    // --- Select variants in Virtual Cohort ---------------------------------------------------------------------------

    @Nested
    @DisplayName("SelectVariantsInRegionInVirtualCohort")
    class selectVariantsInRegionInVirtualCohort {

        @Test
        @DisplayName("Select SNP in HG002, HG003")
        public void selectSNPInRegionInVC() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880239;
            final int end = 883624;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegionInVirtualCohort: select a single SNP in HG002, HG003 in region " +
                        chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegionInVirtualCohort(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesResponse allelesResponse = response.next();
            assertEquals(1, allelesResponse.getAllelesCount());
            Variant variant = allelesResponse.getAlleles(0);
            // verify 1:881627-881627:G->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getChr())),
                () -> assertEquals(881627, variant.getStart()),
                () -> assertEquals(881627, variant.getEnd()),
                () -> assertEquals("G", variant.getRef()),
                () -> assertEquals("A", variant.getAlt()),
                () -> assertEquals(0.8333333f, variant.getAf(), 0.001f),
                () -> assertEquals(5.0f, variant.getAc(), 0.001f),
                () -> assertEquals(6, variant.getAn()),
                () -> assertEquals(2, variant.getHomc()),
                () -> assertEquals(1, variant.getHetc()),
                () -> assertEquals(0, variant.getMisc()));
        }

        @Test
        @DisplayName("Select Hom variants in HG003, HG004")
        public void selectHomVariantsInRegion() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880239;
            final int end = 883624;
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG003", "HG004");
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegionInVirtualCohort: select Hom variants in " + samples + " in region " +
                        chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegionInVirtualCohort(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesResponse allelesResponse = response.next();
            assertEquals(1, allelesResponse.getAllelesCount());
            Variant variant = allelesResponse.getAlleles(0);
            // verify 1:881627-881627:G->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getChr())),
                () -> assertEquals(881627, variant.getStart()),
                () -> assertEquals(881627, variant.getEnd()),
                () -> assertEquals("G", variant.getRef()),
                () -> assertEquals("A", variant.getAlt()),
                () -> assertEquals(0.8333333f, variant.getAf(), 0.001f),
                () -> assertEquals(5.0f, variant.getAc(), 0.001f),
                () -> assertEquals(6, variant.getAn()),
                () -> assertEquals(2, variant.getHomc()),
                () -> assertEquals(1, variant.getHetc()),
                () -> assertEquals(0, variant.getMisc()));
        }

        @Test
        @DisplayName("Select Het variants in HG003, HG004")
        public void selectHetVariantsInRegion() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880238;
            final int end = 880391;
            final boolean selectHom = false;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG003", "HG004");
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegionInVirtualCohort: select Het variants in " + samples + " in region " +
                        chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegionInVirtualCohort(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesResponse allelesResponse = response.next();
            assertEquals(1, allelesResponse.getAllelesCount());
            Variant variant = allelesResponse.getAlleles(0);
            // verify 1:880390-880390:C->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getChr())),
                () -> assertEquals(880390, variant.getStart()),
                () -> assertEquals(880390, variant.getEnd()),
                () -> assertEquals("C", variant.getRef()),
                () -> assertEquals("A", variant.getAlt()),
                () -> assertEquals(0.16666667f, variant.getAf(), 0.001f),
                () -> assertEquals(1f, variant.getAc(), 0.001f),
                () -> assertEquals(6, variant.getAn()),
                () -> assertEquals(0, variant.getHomc()),
                () -> assertEquals(1, variant.getHetc()),
                () -> assertEquals(0, variant.getMisc()));
        }

        @Test
        @DisplayName("Select p53 variants in HG002, HG003")
        public void selectVariantsInP53inVC() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 6;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegionInVirtualCohort: select p53 variants in " +
                        samples + " in region " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegionInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with assembly mismatch")
        public void selectVariantsAssemblyMismatch() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh38;
            Iterator<AllelesResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegionInVirtualCohort: with assembly mismatch");
            response = blockingStub.selectVariantsInRegionInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with default assembly")
        public void selectVariantsDefaultAssembly() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            Iterator<AllelesResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .build();
            logger.info("SelectVariantsInRegionInVirtualCohort: with default assembly");
            response = blockingStub.selectVariantsInRegionInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with empty chromosome")
        public void selectVariantsEmptyChr() {
            final Chromosome chr = Chromosome.CHR_MT;
            final int start = 1;
            final int end = 1;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegionInVirtualCohort:  with empty chromosome");
            response = blockingStub.selectVariantsInRegionInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select empty results")
        public void selectVariantsInRegionInVirtualCohortEmpty() {
            final Chromosome chrX = Chromosome.CHR_X;
            final int startX = 1; // start of chrX
            final int endX = 155270560; // end of chrX
            final boolean selectHom = false;
            final boolean selectHet = false;
            final int expectedAlleles = 0;
            List<String> samples = Arrays.asList("HG002", "HG003", "HG004");
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chrX)
                    .setStart(startX)
                    .setEnd(endX)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegionInVirtualCohort: select chr X with empty results");
            response = blockingStub.selectVariantsInRegionInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }
    }

    // --- Select variants in Virtual Cohort with stats ----------------------------------------------------------------

    @Nested
    @DisplayName("SelectVariantsInRegionInVirtualCohortWithStats")
    class selectVariantsInRegionInVirtualCohortWithStats {

        @Test
        @DisplayName("Select SNP in HG002, HG003, HG004")
        public void selectSNPInRegionInVCwStats() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880391;
            final int end = 883624;
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedAlleles = 1;
            List<String> samples = Arrays.asList("HG002", "HG003", "HG004");
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: select a single SNP in " +
                        samples + " in region " + chr + ":" + start + "-" + end);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesWithStatsResponse allelesResponse = response.next();
            assertEquals(expectedAlleles, allelesResponse.getAllelesCount());
            VariantWithStats variant = allelesResponse.getAlleles(0);
            // verify 1:881627-881627:G->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getAllele().getChr())),
                () -> assertEquals(881627, variant.getAllele().getStart()),
                () -> assertEquals(881627, variant.getAllele().getEnd()),
                () -> assertEquals("G", variant.getAllele().getRef()),
                () -> assertEquals("A", variant.getAllele().getAlt()),
                () -> assertEquals(0.8333333f, variant.getAllele().getAf(), 0.001f),
                () -> assertEquals(5.0f, variant.getAllele().getAc(), 0.001f),
                () -> assertEquals(6, variant.getAllele().getAn()),
                () -> assertEquals(2, variant.getAllele().getHomc()),
                () -> assertEquals(1, variant.getAllele().getHetc()),
                () -> assertEquals(0, variant.getAllele().getMisc()),
                // VC stats - must be identical to whole cohort
                () -> assertEquals(variant.getVaf(), variant.getAllele().getAf(), 0.001f),
                () -> assertEquals(variant.getVac(), variant.getAllele().getAc(), 0.001f),
                () -> assertEquals(variant.getVan(), variant.getAllele().getAn()),
                () -> assertEquals(variant.getVhomc(), variant.getAllele().getHomc()),
                () -> assertEquals(variant.getVhetc(), variant.getAllele().getHetc()));
        }

        @Test
        @DisplayName("Select SNP in HG003, HG004")
        public void selectSNPInRegionInVCwStats2() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880391;
            final int end = 883624;
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedAlleles = 1;
            List<String> samples = Arrays.asList("HG003", "HG004");
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: select a single SNP in " +
                        samples + " in region " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesWithStatsResponse allelesResponse = response.next();
            assertEquals(expectedAlleles, allelesResponse.getAllelesCount());
            VariantWithStats variant = allelesResponse.getAlleles(0);
            // verify 1:881627-881627:G->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getAllele().getChr())),
                () -> assertEquals(881627, variant.getAllele().getStart()),
                () -> assertEquals(881627, variant.getAllele().getEnd()),
                () -> assertEquals("G", variant.getAllele().getRef()),
                () -> assertEquals("A", variant.getAllele().getAlt()),
                () -> assertEquals(0.8333333f, variant.getAllele().getAf(), 0.001f),
                () -> assertEquals(5.0f, variant.getAllele().getAc(), 0.001f),
                () -> assertEquals(6, variant.getAllele().getAn()),
                () -> assertEquals(2, variant.getAllele().getHomc()),
                () -> assertEquals(1, variant.getAllele().getHetc()),
                () -> assertEquals(0, variant.getAllele().getMisc()),
                // VC stats in 2 samples with het and hom GT
                () -> assertEquals(0.75f, variant.getVaf(), 0.001f),
                () -> assertEquals(3f, variant.getVac(), 0.001f),
                () -> assertEquals(4, variant.getVan()),
                () -> assertEquals(1, variant.getVhomc()),
                () -> assertEquals(1, variant.getVhetc()));
        }

        @Test
        @DisplayName("Select Hom variants in HG003, HG004")
        public void selectHomVariantsInRegion() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880239;
            final int end = 883624;
            final boolean selectHom = true;
            final boolean selectHet = false;
            final int expectedAlleles = 1;
            List<String> samples = Arrays.asList("HG003", "HG004");
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: select Hom variants in " +
                        samples + " in region " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesWithStatsResponse allelesResponse = response.next();
            assertEquals(expectedAlleles, allelesResponse.getAllelesCount());
            VariantWithStats variant = allelesResponse.getAlleles(0);
            // verify 1:881627-881627:G->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getAllele().getChr())),
                () -> assertEquals(881627, variant.getAllele().getStart()),
                () -> assertEquals(881627, variant.getAllele().getEnd()),
                () -> assertEquals("G", variant.getAllele().getRef()),
                () -> assertEquals("A", variant.getAllele().getAlt()),
                () -> assertEquals(0.8333333f, variant.getAllele().getAf(), 0.001f),
                () -> assertEquals(5.0f, variant.getAllele().getAc(), 0.001f),
                () -> assertEquals(6, variant.getAllele().getAn()),
                () -> assertEquals(2, variant.getAllele().getHomc()),
                () -> assertEquals(1, variant.getAllele().getHetc()),
                () -> assertEquals(0, variant.getAllele().getMisc()),
                // VC stats in 2 samples with het and hom GT
                () -> assertEquals(0.75f, variant.getVaf(), 0.001f),
                () -> assertEquals(3f, variant.getVac(), 0.001f),
                () -> assertEquals(4, variant.getVan()),
                () -> assertEquals(1, variant.getVhomc()),
                () -> assertEquals(1, variant.getVhetc()));
        }

        @Test
        @DisplayName("Select Het variants in HG003, HG004")
        public void selectHetVariantsInRegion() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 880238;
            final int end = 880391;
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedAlleles = 1;
            List<String> samples = Arrays.asList("HG003", "HG004");
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: select Het variants in " +
                        samples + " in region " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            // assert there is one and only one correct variant in response
            assertTrue(response.hasNext());
            AllelesWithStatsResponse allelesResponse = response.next();
            assertEquals(expectedAlleles, allelesResponse.getAllelesCount());
            VariantWithStats variant = allelesResponse.getAlleles(0);
            // verify 1:880390-880390:C->A
            assertAll(
                () -> assertTrue(chr.equals(variant.getAllele().getChr())),
                () -> assertEquals(880390, variant.getAllele().getStart()),
                () -> assertEquals(880390, variant.getAllele().getEnd()),
                () -> assertEquals("C", variant.getAllele().getRef()),
                () -> assertEquals("A", variant.getAllele().getAlt()),
                () -> assertEquals(0.16666667f, variant.getAllele().getAf(), 0.001f),
                () -> assertEquals(1f, variant.getAllele().getAc(), 0.001f),
                () -> assertEquals(6, variant.getAllele().getAn()),
                () -> assertEquals(0, variant.getAllele().getHomc()),
                () -> assertEquals(1, variant.getAllele().getHetc()),
                () -> assertEquals(0, variant.getAllele().getMisc()),
                // VC stats in 2 samples with single het
                () -> assertEquals(0.25f, variant.getVaf(), 0.001f),
                () -> assertEquals(1f, variant.getVac(), 0.001f),
                () -> assertEquals(4, variant.getVan()),
                () -> assertEquals(0, variant.getVhomc()),
                () -> assertEquals(1, variant.getVhetc()));
        }

        @Test
        @DisplayName("Select p53 variants in HG002, HG003")
        public void selectVariantsInP53inVCwStats() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 6;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: select p53 variants in " +
                        samples + " in region " + chr + ":" + start + "-" + end);
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with assembly mismatch")
        public void selectVariantsAssemblyMismatch() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh38;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: with assembly mismatch");
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with default assembly")
        public void selectVariantsDefaultAssembly() {
            final Chromosome chr = Chromosome.CHR_17;
            final int start = 7565097;
            final int end = 7590856;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .build();
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: with default assembly");
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with empty chromosome")
        public void selectVariantsEmptyChr() {
            final Chromosome chr = Chromosome.CHR_MT;
            final int start = 1;
            final int end = 1;
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: with empty chromosome");
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select empty results")
        public void selectVariantsInRegionInVirtualCohortWithStatsEmpty() {
            final Chromosome chrX = Chromosome.CHR_X;
            final int startX = 1; // start of chrX
            final int endX = 155270560; // end of chrX
            final boolean selectHom = false;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003", "HG004");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInRegionInVCRequest request =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chrX)
                    .setStart(startX)
                    .setEnd(endX)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("selectVariantsInRegionInVirtualCohortWithStats: select chr X with empty results");
            response = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }
    }

    // --- Select variants in a multiple regions -----------------------------------------------------------------------

    @Nested
    @DisplayName("SelectVariantsInMultiRegions")
    class SelectVariantsInMultiRegions {

        @Test
        @DisplayName("Select all p53 & TTN variants")
        public void selectP53andTTNVariants() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedAlleles = 7 + 36;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsRequest request =
                AllelesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInRegion: select all p53 & TTN variants");
            response = blockingStub.selectVariantsInMultiRegions(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select Het p53 & TTN variants")
        public void selectHetVariantsInMultiRegions() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedAlleles = 16;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsRequest request =
                AllelesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegions: select Het p53 & TTN variants");
            response = blockingStub.selectVariantsInMultiRegions(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select Hom p53 & TTN variants")
        public void selectHomVariantsInMultiRegions() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            final int expectedAlleles = 28;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsRequest request =
                AllelesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegions: select Hom p53 & TTN variants");
            response = blockingStub.selectVariantsInMultiRegions(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select in overlapping regions")
        public void selectOverlapping() {
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrTTN, chrTTN);
            List<Integer> starts = Arrays.asList(startTTN, startTTN);
            List<Integer> ends = Arrays.asList(endTTN, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedAlleles = 36;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsRequest request =
                AllelesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegions: select in overlapping regions");
            response = blockingStub.selectVariantsInMultiRegions(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());

            // validate results are the same with a single region selection
            Iterator<AllelesResponse> response2;
            AllelesInRegionRequest request2 =
                AllelesInRegionRequest
                    .newBuilder()
                    .setChr(chrTTN)
                    .setStart(startTTN)
                    .setEnd(endTTN)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            response2 = blockingStub.selectVariantsInRegion(request2);
            assertTrue(response2.hasNext());
            LinkedList<Variant> variants2 = new LinkedList<>();
            while (response2.hasNext()) {
                variants2.addAll(response2.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants2.size());

            Set<Variant> fromDupRegions = new HashSet<>(variants);
            Set<Variant> fromSingleRegion = new HashSet<>(variants2);
            assertTrue(fromDupRegions.equals(fromSingleRegion));
        }

        @Test
        @DisplayName("Select with assembly mismatch")
        public void selectHomVariantsAssemblyMismatch() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh38;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsRequest request =
                AllelesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegions: with assembly mismatch");
            response = blockingStub.selectVariantsInMultiRegions(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with default assembly")
        public void selectHomVariantsDefaultAssembly() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            final int expectedAlleles = 0;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsRequest request =
                AllelesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .build();
            logger.info("SelectVariantsInMultiRegions: with default assembly");
            response = blockingStub.selectVariantsInMultiRegions(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with empty chromosome")
        public void selectHomVariantsEmptyChr() {
            final Chromosome chrP53 = Chromosome.CHR_MT;
            final int startP53 = 1;
            final int endP53 = 1;
            final Chromosome chrTTN = Chromosome.CHR_MT;
            final int startTTN = 1;
            final int endTTN = 1;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsRequest request =
                AllelesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegions: with empty chromosome");
            response = blockingStub.selectVariantsInMultiRegions(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select empty results")
        public void selectVariantsInMultiRegionsEmpty() {
            final Chromosome chr2 = Chromosome.CHR_2;
            final int start2 = 1; // start of chr2
            final int end2 = 243199373; // end of chr2
            final Chromosome chrX = Chromosome.CHR_X;
            final int startX = 1; // start of chrX
            final int endX = 155270560; // end of chrX
            final boolean selectHom = false;
            final boolean selectHet = false;
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsRequest request =
                AllelesInMultiRegionsRequest
                    .newBuilder()
                    .addChr(chr2)
                    .addChr(chrX)
                    .addStart(start2)
                    .addStart(startX)
                    .addEnd(end2)
                    .addEnd(endX)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegions: select chr 2 and X with empty results");
            response = blockingStub.selectVariantsInMultiRegions(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }
    }

    // --- Select variants in a multiple regions in Virtual Cohort -----------------------------------------------------

    @Nested
    @DisplayName("SelectVariantsInMultiRegionsInVirtualCohort")
    class SelectVariantsInMultiRegionsInVirtualCohort {

        @Test
        @DisplayName("Select p53 & TTN variants in HG002, HG003")
        public void selectP53andTTNVariants() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 40;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohort: select p53 & TTN variants in " + samples);
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }


        @Test
        @DisplayName("Select Het p53 & TTN variants in HG002, HG004")
        public void selectHetVariantsInMultiRegions() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = false;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG004");
            final int expectedAlleles = 10;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohort: select Het p53 & TTN variants in " + samples);
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select Hom p53 & TTN variants in HG002, HG003")
        public void selectHomVariantsInMultiRegions() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 27;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohort: select Hom p53 & TTN variants in " + samples);
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select in overlapping regions")
        public void selectOverlapping() {
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrTTN, chrTTN);
            List<Integer> starts = Arrays.asList(startTTN, startTTN);
            List<Integer> ends = Arrays.asList(endTTN, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 22;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohort: select in overlapping regions in " + samples);
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());

            // validate results are the same with a single region selection
            Iterator<AllelesResponse> response2;
            AllelesInRegionInVCRequest request2 =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chrTTN)
                    .setStart(startTTN)
                    .setEnd(endTTN)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            response2 = blockingStub.selectVariantsInRegionInVirtualCohort(request2);
            assertTrue(response2.hasNext());
            LinkedList<Variant> variants2 = new LinkedList<>();
            while (response2.hasNext()) {
                variants2.addAll(response2.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants2.size());

            Set<Variant> fromDupRegions = new HashSet<>(variants);
            Set<Variant> fromSingleRegion = new HashSet<>(variants2);
            assertTrue(fromDupRegions.equals(fromSingleRegion));
        }

        @Test
        @DisplayName("Select with assembly mismatch")
        public void selectAssemblyMismatch() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh38;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohort: with assembly mismatch");
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with default assembly")
        public void selectDefaultAssembly() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohort: with default assembly");
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with empty chromosome")
        public void selectEmptyChr() {
            final Chromosome chrP53 = Chromosome.CHR_MT;
            final int startP53 = 1;
            final int endP53 = 1;
            final Chromosome chrTTN = Chromosome.CHR_MT;
            final int startTTN = 1;
            final int endTTN = 1;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohort: with empty chromosome");
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select empty results")
        public void selectVariantsInMultiRegionsInVirtualCohortEmpty() {
            final Chromosome chr2 = Chromosome.CHR_2;
            final int start2 = 1; // start of chr2
            final int end2 = 243199373; // end of chr2
            final Chromosome chrX = Chromosome.CHR_X;
            final int startX = 1; // start of chrX
            final int endX = 155270560; // end of chrX
            final boolean selectHom = false;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addChr(chr2)
                    .addChr(chrX)
                    .addStart(start2)
                    .addStart(startX)
                    .addEnd(end2)
                    .addEnd(endX)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohort: select chr 2 and X with empty results");
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohort(request);
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }
    }

    // --- Select variants in a multiple regions in Virtual Cohort with stats ------------------------------------------

    @Nested
    @DisplayName("SelectVariantsInMultiRegionsInVirtualCohortWithStats")
    class SelectVariantsInMultiRegionsInVirtualCohortWithStats {

        @Test
        @DisplayName("Select p53 & TTN variants in HG002, HG004")
        public void selectP53andTTNVariants() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG004");
            final int expectedAlleles = 37;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohortWithStats: select p53 & TTN variants in " + samples);
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select Het p53 & TTN variants in HG002, HG004")
        public void selectHetVariantsInMultiRegions() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = false;
            final boolean selectHet = true;
            List<String> samples = Arrays.asList("HG002", "HG004");
            final int expectedAlleles = 10;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohortWithStats: select Het p53 & TTN variants in " + samples);
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select Hom p53 & TTN variants in HG002, HG003")
        public void selectHomVariantsInMultiRegions() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 27;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohortWithStats: select Hom p53 & TTN variants in " + samples);
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select in overlapping regions")
        public void selectOverlapping() {
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrTTN, chrTTN);
            List<Integer> starts = Arrays.asList(startTTN, startTTN);
            List<Integer> ends = Arrays.asList(endTTN, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 22;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohortWithStats: select in overlapping regions in " + samples);
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());

            // validate results are the same with a single region selection
            Iterator<AllelesWithStatsResponse> response2;
            AllelesInRegionInVCRequest request2 =
                AllelesInRegionInVCRequest
                    .newBuilder()
                    .setChr(chrTTN)
                    .setStart(startTTN)
                    .setEnd(endTTN)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            response2 = blockingStub.selectVariantsInRegionInVirtualCohortWithStats(request2);
            assertTrue(response2.hasNext());
            LinkedList<VariantWithStats> variants2 = new LinkedList<>();
            while (response2.hasNext()) {
                variants2.addAll(response2.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants2.size());

            Set<VariantWithStats> fromDupRegions = new HashSet<>(variants);
            Set<VariantWithStats> fromSingleRegion = new HashSet<>(variants2);
            assertTrue(fromDupRegions.equals(fromSingleRegion));
        }

        @Test
        @DisplayName("Select with assembly mismatch")
        public void selectAssemblyMismatch() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh38;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohortWithStats: with assembly mismatch");
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with default assembly")
        public void selectDefaultAssembly() {
            final Chromosome chrP53 = Chromosome.CHR_17;
            final int startP53 = 7565097;
            final int endP53 = 7590856;
            final Chromosome chrTTN = Chromosome.CHR_2;
            final int startTTN = 179390716;
            final int endTTN = 179695529;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohortWithStats: with default assembly");
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select with empty chromosome")
        public void selectEmptyChr() {
            final Chromosome chrP53 = Chromosome.CHR_MT;
            final int startP53 = 1;
            final int endP53 = 1;
            final Chromosome chrTTN = Chromosome.CHR_MT;
            final int startTTN = 1;
            final int endTTN = 1;
            List<Chromosome> chr = Arrays.asList(chrP53, chrTTN);
            List<Integer> starts = Arrays.asList(startP53, startTTN);
            List<Integer> ends = Arrays.asList(endP53, endTTN);
            final boolean selectHom = true;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectVariantsInMultiRegionsInVirtualCohortWithStats: with empty chromosome");
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohortWithStats(request);
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select empty results")
        public void selectVariantsInMultiRegionsInVirtualCohortEmpty() {
            final Chromosome chr2 = Chromosome.CHR_2;
            final int start2 = 1; // start of chr2
            final int end2 = 243199373; // end of chr2
            final Chromosome chrX = Chromosome.CHR_X;
            final int startX = 1; // start of chrX
            final int endX = 155270560; // end of chrX
            final boolean selectHom = false;
            final boolean selectHet = false;
            List<String> samples = Arrays.asList("HG002", "HG003");
            final int expectedAlleles = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<AllelesWithStatsResponse> response;
            AllelesInMultiRegionsInVCRequest request =
                AllelesInMultiRegionsInVCRequest
                    .newBuilder()
                    .addChr(chr2)
                    .addChr(chrX)
                    .addStart(start2)
                    .addStart(startX)
                    .addEnd(end2)
                    .addEnd(endX)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .addAllSamples(samples)
                    .setAssembly(assembly)
                    .build();
            response = blockingStub.selectVariantsInMultiRegionsInVirtualCohortWithStats(request);
            logger.info("SelectVariantsInMultiRegionsInVirtualCohortWithStats: select chr 2 and X with empty results");
            assertTrue(response.hasNext());
            LinkedList<VariantWithStats> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }
    }

    // --- Select variants in all samples in a panel -------------------------------------------------------------------

    @Nested
    @DisplayName("SelectVariantsInPanel")
    class SelectVariantsInPanel {

        @Test
        @DisplayName("Select variants in Mito panel (11 genes)")
        public void selectVariantsInMitoLiverPanel() {
            String panel = "mito11";
            final int expectedAlleles = 31;
            Iterator<AllelesResponse> response;
            AllelesInPanelRequest request =
                AllelesInPanelRequest
                    .newBuilder()
                    .setPanel(panel)
                    .build();
            response = blockingStub.selectVariantsInPanel(request);
            logger.info("SelectVariantsInPanel: select variants in Mito panel (mitochondrial liver disease genes)");
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select variants in Mito panel (374 nuclear genes)")
        public void selectVariantsInMitoNuclearPanel() {
            String panel = "mito374";
            final int expectedAlleles = 1650;
            Iterator<AllelesResponse> response;
            AllelesInPanelRequest request =
                AllelesInPanelRequest
                    .newBuilder()
                    .setPanel(panel)
                    .build();
            response = blockingStub.selectVariantsInPanel(request);
            logger.info("SelectVariantsInPanel: select variants in Mito panel (mitochondrial disorder - 374 nuclear genes)");
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Select variants in Cancer panel (104 genes)")
        public void selectVariantsInCancerPanel() {
            String panel = "cancer104";
            final int expectedAlleles = 610;
            Iterator<AllelesResponse> response;
            AllelesInPanelRequest request =
                AllelesInPanelRequest
                    .newBuilder()
                    .setPanel(panel)
                    .build();
            response = blockingStub.selectVariantsInPanel(request);
            logger.info("SelectVariantsInPanel: select variants in Cancer panel (solid tumours cancer susceptibility, 104 genes)");
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }
    }

    // ---  Sample selection in a region -------------------------------------------------------------------------------

    @Nested
    @DisplayName("SelectSamplesInRegion")
    class SelectSamplesInRegion {

        @Test
        @DisplayName("Select samples with Het alleles")
        public void selectSamplesInRegionHet() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 878314;
            final int end = 887560;
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedSamples = 2;
            RefAssembly assembly = RefAssembly.GRCh37;
            SamplesResponse response;
            SamplesInRegionRequest request =
                SamplesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInRegion: select samples with Het alleles");
            response = blockingStub.selectSamplesInRegion(request);
            assertEquals(expectedSamples, response.getSamplesCount());
            assertTrue(response.getSamplesList().contains("HG002"));
            assertTrue(response.getSamplesList().contains("HG004"));
        }

        @Test
        @DisplayName("Select samples with Hom alleles")
        public void selectSamplesInRegionHom() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 878314;
            final int end = 887559;
            final String ref = "G";
            final String alt = "C";
            final boolean selectHom = true;
            final boolean selectHet = false;
            final int expectedSamples = 1;
            RefAssembly assembly = RefAssembly.GRCh37;
            SamplesResponse response;
            SamplesInRegionRequest request =
                SamplesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setRef(ref)
                    .setAlt(alt)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInRegion: select samples with Hom alleles");
            response = blockingStub.selectSamplesInRegion(request);
            assertEquals(expectedSamples, response.getSamplesCount());
            assertTrue(response.getSamplesList().contains("HG003"));
        }

        @Test
        @DisplayName("Select samples with Alt/Ref alleles")
        public void selectSamplesInRegionAltRef() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 878314;
            final int end = 887560;
            final String ref = "C";
            final String alt = "T";
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedSamples = 1;
            RefAssembly assembly = RefAssembly.GRCh37;
            SamplesResponse response;
            SamplesInRegionRequest request =
                SamplesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setRef(ref)
                    .setAlt(alt)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInRegion: select samples with specific Alt/Ref alleles");
            response = blockingStub.selectSamplesInRegion(request);
            assertEquals(expectedSamples, response.getSamplesCount());
            assertTrue(response.getSamplesList().contains("HG004"));
        }

        @Test
        @DisplayName("Select samples with assembly mismatch")
        public void selectSamplesInRegionAssemblyMismatch() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 878314;
            final int end = 887560;
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedSamples = 0;
            RefAssembly assembly = RefAssembly.GRCh38;
            SamplesResponse response;
            SamplesInRegionRequest request =
                SamplesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInRegion: with assembly mismatch");
            response = blockingStub.selectSamplesInRegion(request);
            assertEquals(expectedSamples, response.getSamplesCount());
        }

        @Test
        @DisplayName("Select samples with default assembly")
        public void selectSamplesInRegionDefaultAssembly() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 878314;
            final int end = 887560;
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedSamples = 0;
            SamplesResponse response;
            SamplesInRegionRequest request =
                SamplesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .build();
            logger.info("SelectSamplesInRegion: with default assembly");
            response = blockingStub.selectSamplesInRegion(request);
            assertEquals(expectedSamples, response.getSamplesCount());
        }

        @Test
        @DisplayName("Select samples with empty chromosome")
        public void selectSamplesInRegionEmptyChr() {
            final Chromosome chr = Chromosome.CHR_MT;
            final int start = 1;
            final int end = 1;
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedSamples = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            SamplesResponse response;
            SamplesInRegionRequest request =
                SamplesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInRegion: with empty chromosome");
            response = blockingStub.selectSamplesInRegion(request);
            assertEquals(expectedSamples, response.getSamplesCount());
        }

        @Test
        @DisplayName("Select empty results")
        public void selectSamplesInRegionEmpty() {
            final Chromosome chr = Chromosome.CHR_1;
            final int start = 878314;
            final int end = 887560;
            final boolean selectHom = false;
            final boolean selectHet = false;
            final int expectedSamples = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            SamplesResponse response;
            SamplesInRegionRequest request =
                SamplesInRegionRequest
                    .newBuilder()
                    .setChr(chr)
                    .setStart(start)
                    .setEnd(end)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInRegion: select empty result");
            response = blockingStub.selectSamplesInRegion(request);
            assertEquals(expectedSamples, response.getSamplesCount());
        }
    }

    // --- Sample selection in multi regions ---------------------------------------------------------------------------

    @Nested
    @DisplayName("SelectSamplesInMultiRegions")
    class SelectSamplesInMultiRegions {

        @Test
        @DisplayName("Select samples with Het alleles")
        public void selectSamplesInMultiRegionsHet() {
            final Chromosome chr_ = Chromosome.CHR_1;
            final int start1 = 878314;
            final int end1 = 881828;
            final int start2 = 881829;
            final int end2 = 887560;
            List<Chromosome> chr = Arrays.asList(chr_, chr_);
            List<Integer> starts = Arrays.asList(start1, start2);
            List<Integer> ends = Arrays.asList(end1, end2);
            final boolean selectHom = false;
            final boolean selectHet = true;
            final int expectedSamples = 2;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<SamplesResponse> response;
            SamplesInMultiRegionsRequest request =
                SamplesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInMultiRegions: with Het alleles");
            response = blockingStub.selectSamplesInMultiRegions(request);
            assertTrue(response.hasNext());
            SamplesResponse samplesResponse = response.next();
            assertEquals(expectedSamples, samplesResponse.getSamplesCount());
            assertTrue(samplesResponse.getSamplesList().contains("HG002"));
            assertTrue(samplesResponse.getSamplesList().contains("HG004"));
        }

        @Test
        @DisplayName("Select samples with Hom alleles")
        public void selectSamplesInMultiRegionsHom() {
            final Chromosome chr_ = Chromosome.CHR_1;
            final int start1 = 878314;
            final int end1 = 881828;
            final int start2 = 881829;
            final int end2 = 887560;
            final String ref = "G";
            final String alt = "C";
            List<Chromosome> chr = Arrays.asList(chr_, chr_);
            List<Integer> starts = Arrays.asList(start1, start2);
            List<Integer> ends = Arrays.asList(end1, end2);
            List<String> refs = Arrays.asList(ref, ref);
            List<String> alts = Arrays.asList(alt, alt);
            final boolean selectHom = true;
            final boolean selectHet = false;
            final int expectedSamples = 1;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<SamplesResponse> response;
            SamplesInMultiRegionsRequest request =
                SamplesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .addAllRef(refs)
                    .addAllAlt(alts)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInMultiRegions: with Hom alleles");
            response = blockingStub.selectSamplesInMultiRegions(request);
            assertTrue(response.hasNext());
            SamplesResponse samplesResponse = response.next();
            assertEquals(expectedSamples, samplesResponse.getSamplesCount());
            assertTrue(samplesResponse.getSamplesList().contains("HG003"));
        }

        @Test
        @DisplayName("Select samples with Alt/Ref alleles")
        public void selectSamplesInMultiRegionsAltRef() {
            final Chromosome chr_ = Chromosome.CHR_1;
            final int start1 = 878314;
            final int end1 = 881828;
            final int start2 = 881829;
            final int end2 = 887560;
            final String ref = "C";
            final String alt = "T";
            List<Chromosome> chr = Arrays.asList(chr_, chr_);
            List<Integer> starts = Arrays.asList(start1, start2);
            List<Integer> ends = Arrays.asList(end1, end2);
            List<String> refs = Arrays.asList(ref, ref);
            List<String> alts = Arrays.asList(alt, alt);
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedSamples = 1;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<SamplesResponse> response;
            SamplesInMultiRegionsRequest request =
                SamplesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .addAllRef(refs)
                    .addAllAlt(alts)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInMultiRegions: with specific Alt/Ref alleles");
            response = blockingStub.selectSamplesInMultiRegions(request);
            assertTrue(response.hasNext());
            SamplesResponse samplesResponse = response.next();
            assertEquals(expectedSamples, samplesResponse.getSamplesCount());
            assertTrue(samplesResponse.getSamplesList().contains("HG004"));
        }

        @Test
        @DisplayName("Select samples with assembly mismatch")
        public void selectSamplesInMultiRegionsAssemblyMismatch() {
            final Chromosome chr_ = Chromosome.CHR_1;
            final int start1 = 878314;
            final int end1 = 881828;
            final int start2 = 881829;
            final int end2 = 887560;
            final String ref = "C";
            final String alt = "T";
            List<Chromosome> chr = Arrays.asList(chr_, chr_);
            List<Integer> starts = Arrays.asList(start1, start2);
            List<Integer> ends = Arrays.asList(end1, end2);
            List<String> refs = Arrays.asList(ref, ref);
            List<String> alts = Arrays.asList(alt, alt);
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedSamples = 0;
            RefAssembly assembly = RefAssembly.GRCh38;
            Iterator<SamplesResponse> response;
            SamplesInMultiRegionsRequest request =
                SamplesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .addAllRef(refs)
                    .addAllAlt(alts)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInMultiRegions: with assembly mismatch");
            response = blockingStub.selectSamplesInMultiRegions(request);
            assertTrue(response.hasNext());
            SamplesResponse samplesResponse = response.next();
            assertEquals(expectedSamples, samplesResponse.getSamplesCount());
        }

        @Test
        @DisplayName("Select samples with default assembly")
        public void selectSamplesInMultiRegionsDefaultAssembly() {
            final Chromosome chr_ = Chromosome.CHR_1;
            final int start1 = 878314;
            final int end1 = 881828;
            final int start2 = 881829;
            final int end2 = 887560;
            final String ref = "C";
            final String alt = "T";
            List<Chromosome> chr = Arrays.asList(chr_, chr_);
            List<Integer> starts = Arrays.asList(start1, start2);
            List<Integer> ends = Arrays.asList(end1, end2);
            List<String> refs = Arrays.asList(ref, ref);
            List<String> alts = Arrays.asList(alt, alt);
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedSamples = 0;
            Iterator<SamplesResponse> response;
            SamplesInMultiRegionsRequest request =
                SamplesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .addAllRef(refs)
                    .addAllAlt(alts)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .build();
            logger.info("SelectSamplesInMultiRegions: with default assembly");
            response = blockingStub.selectSamplesInMultiRegions(request);
            assertTrue(response.hasNext());
            SamplesResponse samplesResponse = response.next();
            assertEquals(expectedSamples, samplesResponse.getSamplesCount());
        }

        @Test
        @DisplayName("Select samples with empty chromosome")
        public void selectSamplesInMultiRegionsEmptyChr() {
            final Chromosome chr_ = Chromosome.CHR_MT;
            final int start1 = 1;
            final int end1 = 1;
            final int start2 = 1;
            final int end2 = 1;
            final String ref = "C";
            final String alt = "T";
            List<Chromosome> chr = Arrays.asList(chr_, chr_);
            List<Integer> starts = Arrays.asList(start1, start2);
            List<Integer> ends = Arrays.asList(end1, end2);
            List<String> refs = Arrays.asList(ref, ref);
            List<String> alts = Arrays.asList(alt, alt);
            final boolean selectHom = true;
            final boolean selectHet = true;
            final int expectedSamples = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<SamplesResponse> response;
            SamplesInMultiRegionsRequest request =
                SamplesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .addAllRef(refs)
                    .addAllAlt(alts)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInMultiRegions: with assembly mismatch");
            response = blockingStub.selectSamplesInMultiRegions(request);
            assertTrue(response.hasNext());
            SamplesResponse samplesResponse = response.next();
            assertEquals(expectedSamples, samplesResponse.getSamplesCount());
        }

        @Test
        @DisplayName("Select empty results")
        public void selectSamplesInMultiRegionsEmpty() {
            final Chromosome chr_ = Chromosome.CHR_1;
            final int start1 = 878314;
            final int end1 = 881828;
            final int start2 = 881829;
            final int end2 = 887560;
            List<Chromosome> chr = Arrays.asList(chr_, chr_);
            List<Integer> starts = Arrays.asList(start1, start2);
            List<Integer> ends = Arrays.asList(end1, end2);
            final boolean selectHom = false;
            final boolean selectHet = false;
            final int expectedSamples = 0;
            RefAssembly assembly = RefAssembly.GRCh37;
            Iterator<SamplesResponse> response;
            SamplesInMultiRegionsRequest request =
                SamplesInMultiRegionsRequest
                    .newBuilder()
                    .addAllChr(chr)
                    .addAllStart(starts)
                    .addAllEnd(ends)
                    .setHom(selectHom)
                    .setHet(selectHet)
                    .setAssembly(assembly)
                    .build();
            logger.info("SelectSamplesInMultiRegions: select empty result");
            response = blockingStub.selectSamplesInMultiRegions(request);
            assertTrue(response.hasNext());
            SamplesResponse samplesResponse = response.next();
            assertEquals(expectedSamples, samplesResponse.getSamplesCount());
        }
    }

    // --- Variant selection by inheritance model ----------------------------------------------------------------------

    @Nested
    @DisplayName("Inheritance Model")
    class InheritanceModel {

        @Test
        @DisplayName("de Novo")
        public void selectDeNovo() {
            final String mother = "HG004";
            final String father = "HG003";
            final String son = "HG002";
            final int expectedAlleles = 548;
            Iterator<AllelesResponse> response;
            DeNovoRequest request =
                DeNovoRequest
                    .newBuilder()
                    .setParent1(mother)
                    .setParent2(father)
                    .setProband(son)
                    .build();
            response = blockingStub.selectDeNovo(request);
            logger.info("SelectDeNovo: de Novo variants in the whole genome");
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Heterozygous dominant")
        public void selectHetDominant() {
            final String mother = "HG004";
            final String father = "HG003";
            final String son = "HG002";
            final int expectedAlleles = 14477;
            Iterator<AllelesResponse> response;
            HetDominantRequest request =
                HetDominantRequest
                    .newBuilder()
                    .setAffectedParent(mother)
                    .setUnaffectedParent(father)
                    .setAffectedChild(son)
                    .build();
            response = blockingStub.selectHetDominant(request);
            logger.info("SelectHetDominant: heterozygous dominant variants in affected child in the whole genome");
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }

        @Test
        @DisplayName("Homozygous recessive")
        public void selectHomRecessive() {
            final String mother = "HG004";
            final String father = "HG003";
            final String son = "HG002";
            final int expectedAlleles = 3415;
            Iterator<AllelesResponse> response;
            HomRecessiveRequest request =
                HomRecessiveRequest
                    .newBuilder()
                    .setUnaffectedParent1(mother)
                    .setUnaffectedParent2(father)
                    .setAffectedChild(son)
                    .build();
            response = blockingStub.selectHomRecessive(request);
            logger.info("SelectHomRecessive: homozygous recessive variants in affected child in the whole genome");
            assertTrue(response.hasNext());
            LinkedList<Variant> variants = new LinkedList<>();
            while (response.hasNext()) {
                variants.addAll(response.next().getAllelesList());
            }
            assertEquals(expectedAlleles, variants.size());
        }
    }

    // --- TopNHWE -----------------------------------------------------------------------------------------------------

    @Nested
    @DisplayName("Top N HWE")
    class TopNHWE {

        @Test
        @DisplayName("1st place")
        public void firstPlace() {
            final int n = 1;
            TopNHWERequest request =
                TopNHWERequest
                    .newBuilder()
                    .setN(n)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNHWE(request);
            logger.info("1st place");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
            assertEquals(0.11999999f, alleles.get(0).getPhwe(), 0.001f);
        }

        @Test
        @DisplayName("1st place sequential execution")
        public void firstPlaceSeq() {
            final int n = 1;
            TopNHWERequest request =
                TopNHWERequest
                    .newBuilder()
                    .setN(n)
                    .setSeq(true)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNHWE(request);
            logger.info("1st place sequential execution");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
            assertEquals(0.11999999f, alleles.get(0).getPhwe(), 0.001f);
        }

        @Test
        @DisplayName("Nth place")
        public void nthPlace() {
            final int n = 17;
            TopNHWERequest request =
                TopNHWERequest
                    .newBuilder()
                    .setN(n)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNHWE(request);
            logger.info("Nth place");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
        }

        @Test
        @DisplayName("Nth place sequential execution")
        public void nthPlaceSeq() {
            final int n = 17;
            TopNHWERequest request =
                TopNHWERequest
                    .newBuilder()
                    .setN(n)
                    .setSeq(true)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNHWE(request);
            logger.info("Nth place sequential execution");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
        }
    }

    // --- TopNchi2 -----------------------------------------------------------------------------------------------------

    @Nested
    @DisplayName("Top N chi-squared")
    class TopNchi2 {

        @Test
        @DisplayName("1st place")
        public void firstPlace() {
            final int n = 1;
            List<String> samples = Arrays.asList("HG002");
            TopNchi2Request request =
                TopNchi2Request
                    .newBuilder()
                    .setN(n)
                    .addAllSamples(samples)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNchi2(request);
            logger.info("1st place");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
            assertEquals(0.1875f, alleles.get(0).getPchi2(), 0.001f);
        }

        @Test
        @DisplayName("1st place sequential execution")
        public void firstPlaceSeq() {
            final int n = 1;
            List<String> samples = Arrays.asList("HG002");
            TopNchi2Request request =
                TopNchi2Request
                    .newBuilder()
                    .setN(n)
                    .addAllSamples(samples)
                    .setSeq(true)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNchi2(request);
            logger.info("1st place sequential execution");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
            assertEquals(0.1875f, alleles.get(0).getPchi2(), 0.001f);
        }

        @Test
        @DisplayName("2nd place")
        public void secondPlace() {
            final int n = 2;
            List<String> samples = Arrays.asList("HG002");
            TopNchi2Request request =
                TopNchi2Request
                    .newBuilder()
                    .setN(n)
                    .addAllSamples(samples)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNchi2(request);
            logger.info("2nd place");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
            assertEquals(0.375f, alleles.get(1).getPchi2(), 0.001f);
        }

        @Test
        @DisplayName("2nd place sequential execution")
        public void secondPlaceSeq() {
            final int n = 2;
            List<String> samples = Arrays.asList("HG002");
            TopNchi2Request request =
                TopNchi2Request
                    .newBuilder()
                    .setN(n)
                    .addAllSamples(samples)
                    .setSeq(true)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNchi2(request);
            logger.info("2nd place sequential execution");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
            assertEquals(0.375f, alleles.get(1).getPchi2(), 0.001f);
        }

        @Test
        @DisplayName("Nth place")
        public void nthPlace() {
            final int n = 17;
            List<String> samples = Arrays.asList("HG002");
            TopNchi2Request request =
                TopNchi2Request
                    .newBuilder()
                    .setN(n)
                    .addAllSamples(samples)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNchi2(request);
            logger.info("Nth place");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
            assertEquals(0.375f, alleles.get(n-1).getPchi2(), 0.001f);
        }

        @Test
        @DisplayName("Nth place sequential execution")
        public void nthPlaceSeq() {
            final int n = 17;
            List<String> samples = Arrays.asList("HG002");
            TopNchi2Request request =
                TopNchi2Request
                    .newBuilder()
                    .setN(n)
                    .addAllSamples(samples)
                    .setSeq(true)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNchi2(request);
            logger.info("Nth place sequential execution");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertEquals(n, alleles.size());
            assertEquals(0.375f, alleles.get(n-1).getPchi2(), 0.001f);
        }

        @Test
        @DisplayName("Samples mismatch: empty results")
        public void empty() {
            final int n = 17;
            List<String> samples = Arrays.asList("HG001");
            TopNchi2Request request =
                TopNchi2Request
                    .newBuilder()
                    .setN(n)
                    .addAllSamples(samples)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNchi2(request);
            logger.info("Samples mismatch: empty results");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertTrue(alleles.isEmpty());
        }

        @Test
        @DisplayName("No samples: empty results")
        public void noSamples() {
            final int n = 17;
            TopNchi2Request request =
                TopNchi2Request
                    .newBuilder()
                    .setN(n)
                    .build();
            AllelesWithStatsResponse response = blockingStub.topNchi2(request);
            logger.info("No samples: empty results");
            List<VariantWithStats> alleles = response.getAllelesList();
            assertTrue(alleles.isEmpty());
        }
    }

    // --- PRS ---------------------------------------------------------------------------------------------------------

    @Nested
//    @Disabled
    @DisplayName("PRS")
    class PRS {

        @Test
        @DisplayName("Atrial fibrillation: all samples")
        public void atrialFibrillation() {
            final String prsName = "Atrial fibrillation";
            List<String> samples = Arrays.asList("HG002", "HG003", "HG004");
            PRSRequest request =
                PRSRequest
                    .newBuilder()
                    .setPrsName(prsName)
                    .addAllSamples(samples)
                    .build();
            PRSResponse response = blockingStub.prs(request);
            logger.info("Atrial fibrillation: all samples");
            final int expectedPRSCardinality = 97;
            assertEquals(expectedPRSCardinality, response.getPrsCardinality());
            List<SampleScore> sampleScores = response.getSampleScoresList();
            assertEquals(3, sampleScores.size());
            for (SampleScore sScore : sampleScores) {
                switch(sScore.getSample()) {
                    case "HG002":
                        assertAll(
                            () -> assertEquals(0.19139999f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(2, sScore.getHethomCardinality()),
                            () -> assertEquals(1, sScore.getRefCardinality()));
                        break;

                    case "HG003":
                        assertAll(
                            () -> assertEquals(0.3622f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(3, sScore.getHethomCardinality()),
                            () -> assertEquals(0, sScore.getRefCardinality()));
                        break;

                    case "HG004":
                        assertAll(
                            () -> assertEquals(0.26319999f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(2, sScore.getHethomCardinality()),
                            () -> assertEquals(1, sScore.getRefCardinality()));
                        break;

                    default: fail("Unknown sample: " + sScore.getSample());
                }
            }
        }

        @Test
        @DisplayName("Atrial fibrillation: cohort")
        public void atrialFibrillationCohort() {
            final String prsName = "Atrial fibrillation";
            final String cohort = "trio";
            List<String> samples = Arrays.asList("HG002");
            PRSRequest request =
                PRSRequest
                    .newBuilder()
                    .setPrsName(prsName)
                    .setCohortName(cohort)
                    .addAllSamples(samples)
                    .build();
            PRSResponse response = blockingStub.prs(request);
            logger.info("Atrial fibrillation: cohort (defaults)");
            final int expectedPRSCardinality = 97;
            assertEquals(expectedPRSCardinality, response.getPrsCardinality());
            List<SampleScore> sampleScores = response.getSampleScoresList();
            assertEquals(3, sampleScores.size());
            for (SampleScore sScore : sampleScores) {
                switch(sScore.getSample()) {
                    case "HG002":
                        assertAll(
                            () -> assertEquals(0.19139999f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(2, sScore.getHethomCardinality()),
                            () -> assertEquals(1, sScore.getRefCardinality()));
                        break;

                    case "HG003":
                        assertAll(
                            () -> assertEquals(0.3622f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(3, sScore.getHethomCardinality()),
                            () -> assertEquals(0, sScore.getRefCardinality()));
                        break;

                    case "HG004":
                        assertAll(
                            () -> assertEquals(0.26319999f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(2, sScore.getHethomCardinality()),
                            () -> assertEquals(1, sScore.getRefCardinality()));
                        break;

                    default: fail("Unknown sample: " + sScore.getSample());
                }
            }
        }

        @Test
        @DisplayName("Atrial fibrillation: dominant")
        public void atrialFibrillationDominant() {
            final String prsName = "Atrial fibrillation";
            final String cohort = "trio";
            final Boolean dominant = true;
            PRSRequest request =
                PRSRequest
                    .newBuilder()
                    .setPrsName(prsName)
                    .setCohortName(cohort)
                    .setDominant(dominant)
                    .build();
            PRSResponse response = blockingStub.prs(request);
            logger.info("Atrial fibrillation: dominant");
            final int expectedPRSCardinality = 97;
            assertEquals(expectedPRSCardinality, response.getPrsCardinality());
            List<SampleScore> sampleScores = response.getSampleScoresList();
            assertEquals(3, sampleScores.size());
            for (SampleScore sScore : sampleScores) {
                switch(sScore.getSample()) {
                    case "HG002":
                        assertAll(
                            () -> assertEquals(0.19139999f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(2, sScore.getHethomCardinality()),
                            () -> assertEquals(1, sScore.getRefCardinality()));
                        break;

                    case "HG003":
                        assertAll(
                            () -> assertEquals(0.2426f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(3, sScore.getHethomCardinality()),
                            () -> assertEquals(0, sScore.getRefCardinality()));
                        break;

                    case "HG004":
                        assertAll(
                            () -> assertEquals(0.19139999f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(2, sScore.getHethomCardinality()),
                            () -> assertEquals(1, sScore.getRefCardinality()));
                        break;

                    default: fail("Unknown sample: " + sScore.getSample());
                }
            }
        }

        @Test
        @DisplayName("Atrial fibrillation: recessive")
        public void atrialFibrillationRecessive() {
            final String prsName = "Atrial fibrillation";
            final String cohort = "trio";
            final Boolean recessive = true;
            PRSRequest request =
                PRSRequest
                    .newBuilder()
                    .setPrsName(prsName)
                    .setCohortName(cohort)
                    .setRecessive(recessive)
                    .build();
            PRSResponse response = blockingStub.prs(request);
            logger.info("Atrial fibrillation: recessive");
            final int expectedPRSCardinality = 97;
            assertEquals(expectedPRSCardinality, response.getPrsCardinality());
            List<SampleScore> sampleScores = response.getSampleScoresList();
            assertEquals(3, sampleScores.size());
            for (SampleScore sScore : sampleScores) {
                switch(sScore.getSample()) {
                    case "HG002":
                        assertAll(
                            () -> assertEquals(0f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(2, sScore.getHethomCardinality()),
                            () -> assertEquals(1, sScore.getRefCardinality()));
                        break;

                    case "HG003":
                        assertAll(
                            () -> assertEquals(0.1196f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(3, sScore.getHethomCardinality()),
                            () -> assertEquals(0, sScore.getRefCardinality()));
                        break;

                    case "HG004":
                        assertAll(
                            () -> assertEquals(0.0718f, sScore.getScoresSum(), 0.001f),
                            () -> assertEquals(2, sScore.getHethomCardinality()),
                            () -> assertEquals(1, sScore.getRefCardinality()));
                        break;

                    default: fail("Unknown sample: " + sScore.getSample());
                }
            }
        }

        @Test
        @DisplayName("Atrial fibrillation: invalid args")
        public void atrialFibrillationInvalidArgs() {
            final String prsName = "Atrial fibrillation";
            final String cohort = "trio";
            final Boolean recessive = true;
            final Boolean dominant = true;
            Boolean exceptionCaught = false;
            try {
                PRSRequest request =
                    PRSRequest
                        .newBuilder()
                        .setPrsName(prsName)
                        .setCohortName(cohort)
                        .setDominant(dominant)
                        .setRecessive(recessive)
                        .build();
                PRSResponse response = blockingStub.prs(request);
                logger.info("Atrial fibrillation: invalid args");
                assertTrue(response != null); // consume response
            } catch (io.grpc.StatusRuntimeException ex) {
                assertTrue(ex.getMessage().equalsIgnoreCase("INVALID_ARGUMENT"));
                exceptionCaught = true;
            } finally {
                assertTrue(exceptionCaught);
            }
        }

        @Test
        @DisplayName("No such PRS: empty results")
        public void noSuchPRSEmpty() {
            final String prsName = "no such PRS";
            final String cohort = "trio";
            PRSRequest request =
                PRSRequest
                    .newBuilder()
                    .setPrsName(prsName)
                    .setCohortName(cohort)
                    .build();
            PRSResponse response = blockingStub.prs(request);
            logger.info("No such PRS: empty results");
            final int expectedPRSCardinality = -1;
            assertEquals(expectedPRSCardinality, response.getPrsCardinality());
            List<SampleScore> sampleScores = response.getSampleScoresList();
            assertTrue(sampleScores.isEmpty());
        }

        @Test
        @DisplayName("Samples mismatch: empty results")
        public void atrialFibrillationEmpty() {
            final String prsName = "Atrial fibrillation";
            final String cohort = "no such cohort";
            List<String> samples = Arrays.asList("no such sample");
            PRSRequest request =
                PRSRequest
                    .newBuilder()
                    .setPrsName(prsName)
                    .setCohortName(cohort)
                    .addAllSamples(samples)
                    .build();
            PRSResponse response = blockingStub.prs(request);
            logger.info("Samples mismatch: empty results");
            final int expectedPRSCardinality = 97;
            assertEquals(expectedPRSCardinality, response.getPrsCardinality());
            List<SampleScore> sampleScores = response.getSampleScoresList();
            assertTrue(sampleScores.isEmpty());
        }
    }

    // --- Sex Mismatch Check ------------------------------------------------------------------------------------------

    @Nested
    @DisplayName("Sex Mismatch Check")
    class SexMismatchCheck {

        @Test
        @DisplayName("Default thresholds")
        public void defaultThresholds() {
            final String cohort = "trio";
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .build();
            SexMismatchResponse response = blockingStub.sexMismatchCheck(request);
            logger.info("Default thresholds");
            List<SampleStat> mismatchMales = response.getMismatchMalesList();
            List<SampleStat> mismatchFemales = response.getMismatchFemalesList();
            assertTrue(mismatchMales.isEmpty());
            assertTrue(mismatchFemales.isEmpty());
        }

        @Test
        @DisplayName("Males mismatch")
        public void malesMismatch() {
            final String cohort = "trio";
            final Float femaleThreshold = 0.92f;
            final Float maleThreshold = 0.92f;
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .setFemaleThreshold(femaleThreshold)
                    .setMaleThreshold(maleThreshold)
                    .build();
            SexMismatchResponse response = blockingStub.sexMismatchCheck(request);
            logger.info("Males mismatch");
            List<SampleStat> mismatchMales = response.getMismatchMalesList();
            List<SampleStat> mismatchFemales = response.getMismatchFemalesList();
            assertEquals(2, mismatchMales.size());
            assertTrue(mismatchFemales.isEmpty());
        }

        @Test
        @DisplayName("Females mismatch")
        public void femalesMismatch() {
            final String cohort = "trio";
            final Float femaleThreshold = -0.92f;
            final Float maleThreshold = -0.92f;
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .setFemaleThreshold(femaleThreshold)
                    .setMaleThreshold(maleThreshold)
                    .build();
            SexMismatchResponse response = blockingStub.sexMismatchCheck(request);
            logger.info("Females mismatch");
            List<SampleStat> mismatchMales = response.getMismatchMalesList();
            List<SampleStat> mismatchFemales = response.getMismatchFemalesList();
            assertTrue(mismatchMales.isEmpty());
            assertEquals(1, mismatchFemales.size());
        }

        @Test
        @DisplayName("sequential execution")
        public void seq() {
            final String cohort = "trio";
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .setSeq(true)
                    .build();
            SexMismatchResponse response = blockingStub.sexMismatchCheck(request);
            logger.info("sequential execution");
            List<SampleStat> mismatchMales = response.getMismatchMalesList();
            List<SampleStat> mismatchFemales = response.getMismatchFemalesList();
            assertTrue(mismatchMales.isEmpty());
            assertTrue(mismatchFemales.isEmpty());
        }

        @Test
        @DisplayName("Samples mismatch: empty results")
        public void empty() {
            final String cohort = "no such cohort";
            List<String> samples = Arrays.asList("no such sample");
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .addAllSamples(samples)
                    .build();
            SexMismatchResponse response = blockingStub.sexMismatchCheck(request);
            logger.info("Samples mismatch: empty results");
            List<SampleStat> mismatchMales = response.getMismatchMalesList();
            List<SampleStat> mismatchFemales = response.getMismatchFemalesList();
            assertTrue(mismatchMales.isEmpty());
            assertTrue(mismatchFemales.isEmpty());
        }
    }

    // --- F-statistics ------------------------------------------------------------------------------------------------

    @Nested
    @DisplayName("F-statistics")
    class FstatX {

        @Test
        @DisplayName("Default thresholds")
        public void defaultThresholds() {
            final String cohort = "trio";
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .build();
            FstatXResponse response = blockingStub.fstatX(request);
            logger.info("Default thresholds");
            List<SampleStat> males = response.getMalesList();
            List<SampleStat> females = response.getFemalesList();
            assertEquals(2, males.size());
            assertEquals(1, females.size());

            for (SampleStat sampleStat: males) {
                switch(sampleStat.getSample()) {
                    case "HG002":
                        assertEquals(0.91015995f, sampleStat.getFStat(), 0.001f);
                        break;

                    case "HG003":
                        assertEquals(0.9105927f, sampleStat.getFStat(), 0.001f);
                        break;

                    default: fail("Unknown sample: " + sampleStat.getSample());
                }
            }

            for (SampleStat sampleStat: females) {
                switch(sampleStat.getSample()) {
                    case "HG004":
                        assertEquals(-0.55327535f, sampleStat.getFStat(), 0.001f);
                        break;

                    default: fail("Unknown sample: " + sampleStat.getSample());
                }
            }
        }

        @Test
        @DisplayName("AAF threshold")
        public void aafThreshold() {
            final String cohort = "trio";
            final Float aaf = 0.2f;
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .setAafThreshold(aaf)
                    .build();
            FstatXResponse response = blockingStub.fstatX(request);
            logger.info("AAF threshold");
            List<SampleStat> males = response.getMalesList();
            List<SampleStat> females = response.getFemalesList();
            assertEquals(2, males.size());
            assertEquals(1, females.size());

            for (SampleStat sampleStat: males) {
                switch(sampleStat.getSample()) {
                    case "HG002":
                        assertEquals(0.9206717f, sampleStat.getFStat(), 0.001f);
                        break;

                    case "HG003":
                        assertEquals(0.9210556f, sampleStat.getFStat(), 0.001f);
                        break;

                    default: fail("Unknown sample: " + sampleStat.getSample());
                }
            }

            for (SampleStat sampleStat: females) {
                switch(sampleStat.getSample()) {
                    case "HG004":
                        assertEquals(-0.560666f, sampleStat.getFStat(), 0.001f);
                        break;

                    default: fail("Unknown sample: " + sampleStat.getSample());
                }
            }
        }

        @Test
        @DisplayName("sequential execution")
        public void seq() {
            final String cohort = "trio";
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .setSeq(true)
                    .build();
            FstatXResponse response = blockingStub.fstatX(request);
            logger.info("sequential execution");
            List<SampleStat> males = response.getMalesList();
            List<SampleStat> females = response.getFemalesList();
            assertEquals(2, males.size());
            assertEquals(1, females.size());
        }

        @Test
        @DisplayName("Samples mismatch: empty results")
        public void fstatXEmpty() {
            final String cohort = "no such cohort";
            List<String> samples = Arrays.asList("no such sample");
            FstatXRequest request =
                FstatXRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .addAllSamples(samples)
                    .build();
            FstatXResponse response = blockingStub.fstatX(request);
            logger.info("Samples mismatch: empty results");
            List<SampleStat> males = response.getMalesList();
            List<SampleStat> females = response.getFemalesList();
            assertTrue(males.isEmpty());
            assertTrue(females.isEmpty());
        }
    }


    // --- Kinship -----------------------------------------------------------------------------------------------------

    @Nested
    @DisplayName("Kinship")
    class Kinship {

        @Test
        @DisplayName("Default thresholds")
        public void defaultThresholds() {
            final String cohort = "trio";
            KinshipRequest request =
                KinshipRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .build();
            KinshipResponse response = blockingStub.kinship(request);
            logger.info("Default thresholds");
            List<Relatedness> rels = response.getRelList();
            assertEquals(3, rels.size());

            for (Relatedness r: rels) {
                switch(r.getSampleA()+r.getSampleB()) {
                    case "HG002HG003":
                        assertEquals(0.24894956f, r.getPhiBwf(), 0.001f);
                        assertTrue(r.getDegree() == KinshipDegree.FIRST_DEGREE);
                        break;

                    case "HG002HG004":
                        assertEquals(0.24890211f, r.getPhiBwf(), 0.001f);
                        assertTrue(r.getDegree() == KinshipDegree.FIRST_DEGREE);
                        break;

                    case "HG003HG004":
                        assertEquals(0.0042102933f, r.getPhiBwf(), 0.001f);
                        assertTrue(r.getDegree() == KinshipDegree.UNRELATED);
                        break;

                    default: fail("Unknown pair: " + r.getSampleA() + " & " + r.getSampleB());
                }
            }
        }

        @Test
        @DisplayName("custom threshold")
        public void customThreshold() {
            final String cohort = "trio";
            final float threshold = 0.2f;
            KinshipRequest request =
                KinshipRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .setThreshold(threshold)
                    .build();
            KinshipResponse response = blockingStub.kinship(request);
            logger.info("Custom threshold");
            List<Relatedness> rels = response.getRelList();
            assertEquals(2, rels.size());
        }

        @Test
        @DisplayName("First degree and closer")
        public void firstDegree() {
            final String cohort = "trio";
            KinshipRequest request =
                KinshipRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .setDegree(KinshipDegree.FIRST_DEGREE)
                    .build();
            KinshipResponse response = blockingStub.kinship(request);
            logger.info("First degree and closer");
            List<Relatedness> rels = response.getRelList();
            assertEquals(2, rels.size());
        }

        @Test
        @DisplayName("sequential execution")
        public void seq() {
            final String cohort = "trio";
            KinshipRequest request =
                KinshipRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .setSeq(true)
                    .build();
            KinshipResponse response = blockingStub.kinship(request);
            logger.info("sequential execution");
            List<Relatedness> rels = response.getRelList();
            assertEquals(3, rels.size());
        }

        @Test
        @DisplayName("Samples mismatch: empty results")
        public void kinshipEmpty() {
            final String cohort = "no such cohort";
            List<String> samples = Arrays.asList("no such sample", "does not exist", "HG002");
            KinshipRequest request =
                KinshipRequest
                    .newBuilder()
                    .setCohortName(cohort)
                    .addAllSamples(samples)
                    .build();
            KinshipResponse response = blockingStub.kinship(request);
            logger.info("Samples mismatch: empty results");
            List<Relatedness> rels = response.getRelList();
            assertTrue(rels.isEmpty());
        }
    }
}