import crawler.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;

import static info.kgeorgiy.java.advanced.crawler.URLUtils.getHost;

public class WebCrawler implements Crawler {
    private final static String USAGE = "Usage: WebCrawler url [depth [downloads [extractors [perHost]]]]. Default value for all params os 1";
    private final Downloader downloader;
    private final int perHost;
    private final ExecutorService executorDownloads;
    private final ExecutorService executorExtractors;
    private ConcurrentMap<String, TaskBufferForHost> taskBufferForHost;

    public WebCrawler(final Downloader downloader, final int downloaders, final int extractors, final int perHost) {
        this.downloader = downloader;
        this.perHost = perHost;
        executorDownloads = Executors.newFixedThreadPool(downloaders);
        executorExtractors = Executors.newFixedThreadPool(extractors);
        taskBufferForHost = new ConcurrentHashMap<>();
    }

    public static void main(final String[] args) {
        if (args.length == 0 || args.length > 5) {
            System.out.println(USAGE);
            return;
        }

        try {
            final int depth = Integer.parseInt(getArgument(args, 1));
            final int dowloaders = Integer.parseInt(getArgument(args, 2));
            final int extractors = Integer.parseInt(getArgument(args, 3));
            final int perHost = Integer.parseInt(getArgument(args, 4));

            try (final WebCrawler webCrawler = new WebCrawler(new CachingDownloader(), dowloaders, extractors, perHost)) {
                final Result res = webCrawler.download(args[0], depth);
                final List<String> down = res.getDownloaded();
                for (final String d : down) {
                    System.out.println(d);
                }
            }
        } catch (final IOException e) {
            System.err.println("Couldn't initialize downloader: " + e.getMessage());
        } catch (final NumberFormatException e) {
            System.err.println("Expected numeric arguments only\n" + USAGE);
        }
    }

    static private String getArgument(final String[] args, final int index) {
        if (index >= args.length) {
            return "1";
        } else {
            return args[index];
        }
    }

    private class TaskBufferForHost {
        private int currentTasks;
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        synchronized void runTask() {
            if (currentTasks < perHost && !tasks.isEmpty()) {
                final Runnable task = tasks.poll();
                currentTasks++;
                executorDownloads.submit(() -> {
                    try {
                        task.run();
                    } finally {
                        currentTasks--;
                        runTask();
                    }
                });
            }
        }

        synchronized void addTask(final Runnable task) {
            tasks.add(task);
            runTask();
        }
    }

    private class Solver {
        Queue<String> queue = new ConcurrentLinkedQueue<>();
        Queue<String> queueCopy = new ConcurrentLinkedQueue<>();
        Set<String> unique = new HashSet<>();
        Queue<String> result = new ConcurrentLinkedQueue<>();
        ConcurrentMap<String, IOException> errors = new ConcurrentHashMap<>();

        Solver(final String url) {
            queue.add(url);
        }

        Result run(final int depth) {
            for (int i = 0; i < depth; i++) {
                Queue<String> queueTmp = queue;
                queue = queueCopy;
                queueCopy = queueTmp;

                final Phaser layer = new Phaser(1);
                queue.clear();
                final int finalI = i;
                queueCopy.stream()
                        .filter(unique::add)
                        .forEach(url -> download(url, depth - finalI, layer));
                layer.arriveAndAwaitAdvance();
            }
            return new Result(new ArrayList<>(result), errors);
        }

        private void download(final String url, final int depthLeft, final Phaser phaser) {
            final String host;
            try {
                host = getHost(url);
            } catch (final MalformedURLException e) {
                errors.put(url, e);
                return;
            }

            phaser.register();
            taskBufferForHost.computeIfAbsent(host, s -> new TaskBufferForHost()).addTask(() -> {
                try {
                    final Document document = downloader.download(url);
                    result.add(url);
                    if (depthLeft > 1) {
                        extract(document, phaser);
                    }
                } catch (final IOException e) {
                    errors.put(url, e);
                } finally {
                    phaser.arrive();
                }
            });
        }

        private void extract(final Document document, final Phaser lock) {
            lock.register();
            executorExtractors.submit(() -> {
                try {
                    queue.addAll(document.extractLinks());
                } catch (final IOException ignored) {
                } finally {
                    lock.arrive();
                }
            });
        }

    }

    @Override
    public Result download(final String url, final int depth) {
        return new Solver(url).run(depth);
    }

    @Override
    public void close() {
        executorDownloads.shutdown();
        executorExtractors.shutdown();
        try {
            executorExtractors.awaitTermination(0, TimeUnit.MILLISECONDS);
            executorDownloads.awaitTermination(0, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            System.err.println("Could not terminate executor pools: " + e.getMessage());
        }
    }
}

