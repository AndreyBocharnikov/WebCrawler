import crawler.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;

import static crawler.URLUtils.getHost;

public class WebCrawler implements Crawler {
    private final static String USAGE = "Usage: WebCrawler url [depth [downloads [extractors [perHost]]]]. Default value for all params os 1";
    private final Downloader downloader;
    private final int perHost;
    private final ExecutorService executorDownloads;
    private final ExecutorService executorExtractors;
    private ConcurrentMap<String, TaskBufferForHost> taskBufferForHostMap;

    public WebCrawler(Downloader downloader, int downloaders, int extractors, int perHost) {
        this.downloader = downloader;
        this.perHost = perHost;
        executorDownloads = Executors.newFixedThreadPool(downloaders);
        executorExtractors = Executors.newFixedThreadPool(extractors);
        taskBufferForHostMap = new ConcurrentHashMap<>();
    }

    public static void main(String[] args) {
        if (args.length == 0 || args.length > 5) {
            System.out.println(USAGE);
            return;
        }

        try {
            int depth = Integer.decode(getArgument(args, 1));
            int dowloaders = Integer.decode(getArgument(args, 2));
            int extractors = Integer.decode(getArgument(args, 3));
            int perHost = Integer.decode(getArgument(args, 4));

            WebCrawler webCrawler = new WebCrawler(new CachingDownloader(), dowloaders, extractors, perHost);
            Result res = webCrawler.download(args[0], depth);
            List<String> down = res.getDownloaded();
            for (String d : down) {
                System.out.println(d);
            }
            webCrawler.close();

        } catch (IOException e) {
            System.err.println("Couldn't initialize downloader: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Expected numeric arguments only\n" + USAGE);
        }
    }

    static private String getArgument(String[] args, int index) {
        if (index >= args.length) {
            return "1";
        } else {
            return args[index];
        }
    }

    private class TaskBufferForHost {
        private int currentTasks;
        private Queue<Runnable> tasks;

        TaskBufferForHost() {
            currentTasks = 0;
            tasks = new ArrayDeque<>();
        }

        synchronized void runTask(boolean finished) {
            if (finished) {
                currentTasks--;
            }

            if (currentTasks < perHost) {
                Runnable task = tasks.poll();
                if (task == null) {
                    return;
                }
                currentTasks++;
                executorDownloads.submit(() -> {
                    try {
                        task.run();
                    } finally {
                        runTask(true);
                    }
                });
            }
        }

        synchronized void addTask(Runnable task) {
            tasks.add(task);
            runTask(false);
        }

    }

    private class Solver {
        Queue<String> queue = new ConcurrentLinkedQueue<>();
        Set<String> unique = new HashSet<>();
        Queue<String> result = new ConcurrentLinkedQueue<>();
        ConcurrentMap<String, IOException> errors = new ConcurrentHashMap<>();
        Phaser bfsDone = new Phaser(1);

        Solver(String url, int depth) {
            queue.add(url);
            run(depth);
        }

        void run(int depth) {
            bfsDone.register();
            for (int i = 0; i < depth; i++) {
                List<String> temp = List.copyOf(queue);
                final Phaser layer = new Phaser(1);
                queue.clear();
                int finalI = i;
                temp.stream().filter(unique::add).forEach(url -> download(url, depth - finalI, layer));
                layer.arriveAndAwaitAdvance();
            }
            bfsDone.arrive();
        }

        private void download(String url, int depthLeft, Phaser lock) {
            String host;
            try {
                host = getHost(url);
            } catch (MalformedURLException e) {
                errors.put(url, e);
                return;
            }

            lock.register();
            TaskBufferForHost currentHostTask = taskBufferForHostMap.computeIfAbsent(host, s -> new TaskBufferForHost());
            currentHostTask.addTask(() -> {
                try {
                    Document document = downloader.download(url);
                    result.add(url);
                    if (depthLeft > 1) {
                        extract(document, lock);
                    }
                } catch (IOException e) {
                    errors.put(url, e);
                } finally {
                    lock.arrive();
                }
            });
        }

        private void extract(final Document document, Phaser lock) {
            lock.register();
            executorExtractors.submit(() -> {
                try {
                    List<String> currentResult = document.extractLinks();
                    queue.addAll(currentResult);
                } catch (IOException ignored) {
                } finally {
                    lock.arrive();
                }
            });
        }

        Result getAnswer() {
            bfsDone.arriveAndAwaitAdvance();
            return new Result(new ArrayList<>(result), errors);
        }
    }

    @Override
    public Result download(String url, int depth) {
        return new Solver(url, depth).getAnswer();
    }

    @Override
    public void close() {
        executorDownloads.shutdown();
        executorExtractors.shutdown();
        try {
            executorExtractors.awaitTermination(0, TimeUnit.MILLISECONDS);
            executorDownloads.awaitTermination(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.err.println("Could not terminate executor pools: " + e.getMessage());
        }
    }
}
